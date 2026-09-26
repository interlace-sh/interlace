"""Apply a plan: build changed snapshots, repoint environment views, promote.

For each backfill the model's query has its upstream references rewritten to the
upstreams' physical tables, the strategy emits the build statements, and the
engine runs them; the new snapshot is persisted. Then the environment's virtual
views are repointed at the new physical tables, and the environment is promoted
to the full desired fingerprint set. Builds are DAG-scheduled: each model starts
as soon as its in-plan ancestors finish (bounded by ``parallelism``), so upstream
physical tables always exist before a downstream model builds against them.

Cancellation / sibling failure (``asyncio.TaskGroup`` abort, worker lease loss)
stops promotion: views stay on the previous fingerprints. Mid-strategy warehouse
writes for the cancelled model may already have landed as an unreferenced
physical table — the next successful apply rebuilds the fingerprint, and
``interlace gc`` reclaims orphans after the grace period.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pyarrow as pa
import sqlglot
from sqlglot import exp

from interlace.checks.runner import CheckOutcome, run_checks
from interlace.contracts import validate_contract
from interlace.dsl.dynamic import capturing_registrations
from interlace.engines.base import EngineAdapter, statement_of
from interlace.engines.registry import EngineRegistry, as_registry
from interlace.exceptions import CheckError, ExecutionError, InterlaceError, PlanError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.relation import SqlRelation, TableRef, drop
from interlace.physical.reconcile import model_objects, object_changes, reconcile_statements
from interlace.physical.spec import PhysicalObject
from interlace.plan.plan import XFER_SCHEMA, BackfillTask, ChangeType, Plan, env_view, staging_table
from interlace.plan.resolve import resolve_model_query
from interlace.runtime.python_model import build_python_model, run_python_model
from interlace.sinks import file_statements, target_ref
from interlace.state.interval import Interval
from interlace.state.snapshot import Snapshot
from interlace.state.store import StateStore
from interlace.strategies import Incremental, Strategy, resolve_strategy
from interlace.strategies.base import RowCounts
from interlace.strategies.hash_merge import HashMerge


@dataclass
class ApplyResult:
    built: list[str] = field(default_factory=list)
    reused: list[str] = field(default_factory=list)  # recorded over their previous physical table
    gated: list[str] = field(default_factory=list)  # terminals recorded but not delivered (environment gate)
    transfers: list[str] = field(default_factory=list)  # executed cross-engine transfers
    promoted: int = 0
    checks: list[CheckOutcome] = field(default_factory=list)
    # Wall-clock build seconds per built model (extraction + strategy + checks).
    timings: dict[str, float] = field(default_factory=dict)
    # What each build did to its target's rows, as its strategy interprets the
    # engine's affected-row counts. Interval windows accumulate.
    rows: dict[str, RowCounts] = field(default_factory=dict)
    registered: list[str] = field(default_factory=list)  # models a Python model registered while building

    def record_rows(self, name: str, counts: RowCounts) -> None:
        self.rows[name] = self.rows.get(name, RowCounts()) + counts


# The widening promotions DuckLake's ALTER COLUMN supports, in order.
_NUMERIC_WIDTH = {"TINYINT": 0, "SMALLINT": 1, "INTEGER": 2, "BIGINT": 3, "FLOAT": 4, "DOUBLE": 5}


def _widens(current: str, incoming: str) -> bool:
    """True when ``incoming`` is a strictly wider numeric type than ``current``."""
    return (
        current in _NUMERIC_WIDTH and incoming in _NUMERIC_WIDTH and _NUMERIC_WIDTH[incoming] > _NUMERIC_WIDTH[current]
    )


logger = logging.getLogger("interlace.apply")

ProgressCallback = Callable[[str, str, dict[str, Any]], None]


def _build_detail(result: ApplyResult, name: str) -> dict[str, Any]:
    """What a finished model did: wall-clock seconds and the row delta."""
    detail: dict[str, Any] = {"seconds": round(result.timings.get(name, 0.0), 3)}
    counts = result.rows.get(name)
    if counts is not None:
        detail["rows"] = {"inserted": counts.inserted, "updated": counts.updated, "deleted": counts.deleted}
    return detail


def _failure_detail(exc: BaseException) -> dict[str, Any]:
    """The message and, when an engine statement failed, the SQL that failed."""
    if isinstance(exc, InterlaceError):
        message = exc.message
    else:
        message = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
    detail: dict[str, Any] = {"message": message}
    statement = statement_of(exc)
    if statement:
        detail["statement"] = statement
    return detail


def _resolve_export_path(base_path: Path | None, path: str) -> str:
    from interlace.sinks import expand_path_tokens

    root = base_path or Path.cwd()
    target = Path(expand_path_tokens(path, workspace=root.name))
    if target.is_absolute():
        return str(target)
    return str(root / target)


async def _merge_python_output(
    model: CompiledModel,
    engine: EngineAdapter,
    target: TableRef,
    reader: pa.RecordBatchReader,
    *,
    exists: bool,
    interval: Interval | None = None,
    bootstrap: bool = False,
) -> tuple[RowCounts, Interval | None]:
    """Stage a Python model's Arrow output and apply its keyed strategy in SQL.

    The output lands in a stage table (CREATE OR REPLACE, so a crashed run's
    leftover is harmless), the target is evolved additively when the output grew
    new columns, and the strategy's statements run atomically against a source
    select aligned to the target's column set. The stage is dropped in the same
    batch.
    """
    stage = replace(target, name=f"{target.name}__stage")
    await engine.load(stage, reader, "create")
    stage_table = stage.to_expr()

    strategy = resolve_strategy(model.materialise, model.strategy, model.key, model.time_column)
    source: exp.Query = exp.select("*").from_(stage_table.copy())
    pre_statements: list[exp.Expr] = []
    columns: list[str] | None = None
    if exists:
        alignment = await _align_stage_to_target(engine, stage, target, strategy)
        pre_statements = alignment.pre_statements
        source, columns = alignment.for_strategy(strategy)
    elif isinstance(strategy, HashMerge):
        # hash_merge builds its _hash from the column list; on the first build the target
        # doesn't exist yet (so no align pass ran), so take the columns from the staged output
        columns = [c for c in await engine.describe(stage) if c not in strategy.managed_columns]

    relation = SqlRelation(ast=source)
    if isinstance(strategy, Incremental) and bootstrap:
        # First build of a keyed incremental Python model: the range comes from the
        # staged output, since there is no query to probe the way a SQL model has.
        interval = await _bootstrap_window(model, exp.select("*").from_(stage_table.copy()), engine)
    statements = strategy.plan_statements(relation, target, engine.caps, interval, columns)
    drop_stage = drop(stage_table.copy(), kind="TABLE")
    counts = await engine.execute_all([*pre_statements, *statements, drop_stage])
    written = strategy.row_counts(counts[len(pre_statements) : len(pre_statements) + len(statements)])
    return written, interval


@dataclass(frozen=True)
class Alignment:
    """A staged source fitted to an existing target, in both widths.

    ``source``/``columns`` cover the target's full column set — what a strategy that
    binds positionally or compares whole rows (``INSERT ... SELECT *``, ``EXCEPT``)
    needs, with columns the model doesn't produce NULL-filled in.
    ``produced_source``/``produced`` cover only the model's own columns; a strategy
    that names its columns takes these instead, so the rest of the target is left
    alone on an update and takes its DEFAULTs on an insert. ``unproduced`` names the
    difference — target columns this model has no value for."""

    pre_statements: list[exp.Expr]
    source: exp.Query
    columns: list[str]
    produced_source: exp.Query
    produced: list[str]
    unproduced: list[str]
    added: list[str] = field(default_factory=list)  # columns the model has that the target does not
    casts: list[str] = field(default_factory=list)  # type mismatches that are not numeric widens

    def for_strategy(self, strategy: Strategy) -> tuple[exp.Query, list[str]]:
        """The (source, column list) pair this strategy should be planned against."""
        if strategy.writes_named_columns:
            return self.produced_source, self.produced
        return self.source, self.columns


async def _align_stage_to_target(
    engine: EngineAdapter, stage: TableRef, target: TableRef, strategy: Strategy
) -> Alignment:
    """Align a staged source to an EXISTING target: additive ALTERs for new columns,
    widening type promotions in place, and projections over the stage fitted to the
    target's final column set (NULL-fill vanished columns, cast type drift).

    The strategy's managed bookkeeping columns (e.g. scd's validity pair, hash_merge's
    ``_hash``) live on the target but never in the model's output, so they are held out
    of both projections — the strategy owns them, and must find them already there."""
    target_columns = await engine.describe(target)
    _require_managed_columns(strategy, target, target_columns)
    exclude = strategy.managed_columns
    for column in exclude:
        target_columns.pop(column, None)
    stage_columns = await engine.describe(stage)
    if clash := [c for c in stage_columns if c in exclude]:
        raise PlanError(
            f"model output column {clash[0]!r} collides with a column the "
            f"{_strategy_name(strategy)} strategy manages — rename it in the model",
            details={"target": target.to_expr().sql(), "columns": clash},
        )
    target_expr = target.to_expr()
    pre_statements: list[exp.Expr] = []
    added: list[str] = []
    for column, dtype in stage_columns.items():
        if column not in target_columns:
            added.append(column)
            pre_statements.append(
                exp.Alter(
                    this=target_expr.copy(),
                    kind="TABLE",
                    actions=[
                        exp.ColumnDef(this=exp.to_identifier(column), kind=exp.DataType.build(dtype)),
                    ],
                )
            )
            target_columns[column] = dtype
        elif dtype != target_columns[column] and _widens(target_columns[column], dtype):
            # Source type drifted wider (int -> bigint -> double): promote the target
            # in place — DuckLake supports exactly these widening promotions.
            pre_statements.append(
                exp.Alter(
                    this=target_expr.copy(),
                    kind="TABLE",
                    actions=[exp.AlterColumn(this=exp.column(column), dtype=exp.DataType.build(dtype))],
                )
            )
            target_columns[column] = dtype
    # Any remaining type mismatch (e.g. a numeric field arriving as VARCHAR) is cast
    # to the target's type — deterministic, and loudly fails the run on values that
    # genuinely don't convert rather than silently corrupting the column.
    projection: list[exp.Expr] = []
    produced_projection: list[exp.Expr] = []
    produced: list[str] = []
    unproduced: list[str] = []
    casts: list[str] = []
    for column, dtype in target_columns.items():
        if column not in stage_columns:
            projection.append(exp.alias_(exp.Cast(this=exp.Null(), to=exp.DataType.build(dtype)), column))
            unproduced.append(column)
            continue
        if stage_columns[column] != dtype:
            casts.append(f"{column} {stage_columns[column]} -> {dtype}")
            fitted: exp.Expr = exp.alias_(exp.Cast(this=exp.column(column), to=exp.DataType.build(dtype)), column)
        else:
            fitted = exp.column(column)
        projection.append(fitted)
        produced_projection.append(fitted.copy())
        produced.append(column)
    return Alignment(
        pre_statements=pre_statements,
        source=exp.select(*projection).from_(stage.to_expr()),
        columns=list(target_columns),
        produced_source=exp.select(*produced_projection).from_(stage.to_expr()),
        produced=produced,
        unproduced=unproduced,
        added=added,
        casts=casts,
    )


def _strategy_name(strategy: Strategy) -> str:
    """The strategy's config keyword (``HashMerge`` -> ``hash_merge``), for messages."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", type(strategy).__name__).lower()


def _require_managed_columns(strategy: Strategy, target: TableRef, target_columns: Mapping[str, str]) -> None:
    """A strategy that keeps bookkeeping columns can only take over a table that already
    carries them. Missing ones would otherwise surface as an engine binder error deep in
    the strategy's UPDATE — most likely an externally-owned table that interlace did not
    create, or a strategy swapped under an existing delivery target.

    Nothing to say when the target describes empty: it isn't really there (a snapshot row
    can outlive its table), so the caller is not taking over anything. Matching is
    case-insensitive — an engine that folds unquoted identifiers (Snowflake, BigQuery)
    stores ``_hash`` as ``_HASH`` and would otherwise trip this on every run."""
    if not target_columns:
        return
    present = {c.casefold() for c in target_columns}
    missing = [c for c in strategy.managed_columns if c.casefold() not in present]
    if missing:
        name = _strategy_name(strategy)
        qualified = target.to_expr().sql()
        raise PlanError(
            f"{name} needs its bookkeeping column(s) {', '.join(missing)} on {qualified}, which already "
            f"exists without them — interlace did not create this table. Use strategy: merge, or add "
            f"the column(s) to it.",
            details={"target": qualified, "missing": missing, "strategy": name},
        )


async def _physical_ddl(
    engine: EngineAdapter,
    model: CompiledModel,
    table: TableRef,
    previous: tuple[PhysicalObject, ...],
    *,
    same_table: bool,
) -> tuple[list[exp.Expr], tuple[PhysicalObject, ...], list[str]]:
    """Statements that reconcile interlace-owned indexes and constraints, plus the set to record.

    ``same_table`` is false for a brand-new snapshot table: the previous table's
    indexes die with it (or with gc) and must not be dropped here.
    """
    desired, warnings = model_objects(model, engine.caps)
    drops = object_changes(desired, previous)[1] if same_table else ()
    existing = set(await engine.list_constraints(table)) if same_table else set()
    statements = reconcile_statements(table, model, desired, drops, existing_constraints=existing)
    return statements, desired, warnings


def _remember(notes: list[str] | None, warning: str) -> None:
    if notes is not None and warning not in notes:
        notes.append(warning)


async def _deliver_table(
    model: CompiledModel,
    engine: EngineAdapter,
    resolved: exp.Query,
    strategy: Strategy,
    interval: Interval | None,
    previous: tuple[PhysicalObject, ...] = (),
    notes: list[str] | None = None,
) -> tuple[RowCounts, tuple[PhysicalObject, ...]]:
    """Deliver ``resolved`` into an external table (``materialise: table``) via
    ``strategy`` (replace / append / merge / full_merge / incremental).

    The external target is never dropped (grants and readers survive). When it already
    exists the source is staged in the warehouse and aligned to the target (additive
    ALTERs, widening, casts) so a model that grows or reorders columns evolves the
    destination instead of breaking it or positionally corrupting it. ``schema.columns``
    selects that behaviour: ``additive`` (default), ``reject`` (fail before writing if
    the live table is not a compatible superset), or ``ignore`` (no ALTER; the insert
    fails if it does not fit). A strategy that names its columns is then planned against
    the model's own columns alone, leaving the rest of the target untouched (see
    :class:`Alignment`); a whole-row one gets the source widened to the target with
    NULLs, and binds positionally in the target's order.

    Indexes and constraints interlace recorded are reconciled in the same transaction
    as the delivery. Anything else on the table is left alone.

    Two cases skip staging and run the strategy directly against the target: the first
    delivery (the ensure-create matches the source), and any windowed ``incremental``
    delivery (``interval`` set). An incremental window is grain-scoped and stays
    schema-stable within a fingerprint, so staging the *whole* source once per window
    would make a wide backfill/restate O(windows × source) — the pathological case."""
    target = target_ref(model.target or "")
    exists = await engine.table_exists(target)
    policy = model.schema_policy.columns
    if interval is not None or not exists:
        if exists and interval is not None and policy == "reject" and model.columns:
            from interlace.physical.drift import column_drift

            blocking = [
                note.message
                for note in column_drift(
                    model.name, model.target or "", policy, await engine.describe(target), model.columns
                )
                if note.blocking
            ]
            if blocking:
                raise PlanError(
                    f"{model.name}: schema.columns is reject — {'; '.join(blocking)}",
                    details={"model": model.name, "target": model.target},
                )
        ddl, objects, warnings = await _physical_ddl(engine, model, target, previous, same_table=exists)
        for warning in warnings:
            _remember(notes, warning)
        statements = strategy.plan_statements(SqlRelation(ast=resolved), target, engine.caps, interval)
        counts = await engine.execute_all([*statements, *ddl])
        return strategy.row_counts(counts[: len(statements)]), objects
    stage = TableRef(schema=XFER_SCHEMA, name=f"{model.name}__sink_stage")
    await engine.create_schema(stage.schema)
    await engine.execute(exp.Create(this=stage.to_expr(), kind="TABLE", replace=True, expression=resolved.copy()))
    try:
        pre_statements: list[exp.Expr]
        aligned: exp.Query
        columns: list[str] | None
        if policy == "ignore":
            stage_map = await engine.describe(stage)
            target_columns = await engine.describe(target)
            _require_managed_columns(strategy, target, target_columns)
            extras = [
                column
                for column in target_columns
                if column not in stage_map and column not in strategy.managed_columns
            ]
            if extras:
                _remember(
                    notes,
                    f"{model.name}: {target.to_expr().sql()} has columns the model does not produce "
                    f"({', '.join(extras)}); left in place",
                )
            pre_statements = []
            aligned = exp.select("*").from_(stage.to_expr())
            columns = list(stage_map) if strategy.writes_named_columns else None
        else:
            alignment = await _align_stage_to_target(engine, stage, target, strategy)
            if policy == "reject" and (alignment.added or alignment.casts):
                missing = ", ".join(alignment.added) or "none"
                drifted = ", ".join(alignment.casts) or "none"
                raise PlanError(
                    f"{model.name}: schema.columns is reject and {target.to_expr().sql()} is not a compatible "
                    f"superset (missing columns: {missing}; type drift: {drifted})",
                    details={"model": model.name, "target": model.target},
                )
            pre_statements = alignment.pre_statements
            aligned, columns = alignment.for_strategy(strategy)
            if alignment.unproduced:
                _remember(
                    notes,
                    f"{model.name}: {target.to_expr().sql()} has columns the model does not produce "
                    f"({', '.join(alignment.unproduced)}); left in place",
                )
            if alignment.unproduced and not strategy.writes_named_columns:
                # A whole-row strategy rewrites rows entire, so any column the model doesn't
                # produce is reset on every run — and the deleting ones (replace, full_merge)
                # drop rows it doesn't supply at all. Fine when interlace owns the table (this
                # is just a widened source); destructive when it shares it with another writer.
                logger.warning(
                    "%s: strategy %s writes whole rows into %s — it resets columns this model does not "
                    "produce (%s) on every delivery, and (replace, full_merge) deletes rows it does not "
                    "supply. Use merge, hash_merge or append if another writer owns part of this table.",
                    model.name,
                    model.strategy,
                    target.to_expr().sql(),
                    ", ".join(alignment.unproduced),
                )
        ddl, objects, warnings = await _physical_ddl(engine, model, target, previous, same_table=True)
        for warning in warnings:
            _remember(notes, warning)
        statements = strategy.plan_statements(SqlRelation(ast=aligned), target, engine.caps, interval, columns)
        # One transaction may write only ONE attached database: the delivery batch writes the
        # external target; the stage lives in the warehouse and is dropped separately (a
        # leftover is harmless — the next delivery CREATE OR REPLACEs it).
        counts = await engine.execute_all([*pre_statements, *ddl, *statements])
    finally:
        await engine.execute(drop(stage.to_expr(), kind="TABLE"))
    return strategy.row_counts(counts[len(pre_statements) + len(ddl) :]), objects


async def _stage_cross_engine_inputs(
    model: CompiledModel,
    compiled: CompiledProject,
    registry: EngineRegistry,
    physical: Mapping[str, TableRef],
    staged: set[tuple[str, str]],
    stage_lock: asyncio.Lock,
    result: ApplyResult,
) -> dict[str, TableRef]:
    """Move cross-engine upstreams into staging tables on the model's engine.

    Returns the model's *local* resolution map: cross-engine deps point at their
    staged copies; everything else keeps the global physical map. Each
    (upstream, target-engine) pair transfers once per apply — always replaced,
    so a re-run upstream (merge/incremental) is never read stale. The lock is
    held across the transfer so a concurrent consumer of the same upstream
    never reads a half-populated stage table.
    """
    local = dict(physical)
    for dep in model.dependencies:
        upstream = compiled.models[dep]
        if upstream.engine == model.engine or upstream.materialise == "ephemeral":
            continue
        stage = staging_table(dep)
        local[dep] = stage
        key = (dep, model.engine)
        async with stage_lock:
            if key in staged:
                continue
            target = registry.require(model.engine, model=model.name)
            origin = physical.get(dep, upstream.physical_table)
            await target.create_schema(stage.schema)
            via = "arrow"
            if await _attach_transfer(
                target, registry.attach_uris.get(upstream.engine), upstream.engine, origin, stage
            ):
                via = "attach"  # federated CTAS: no Python hop at all
            else:
                source_engine = registry.require(upstream.engine, model=dep)
                reader = await source_engine.fetch(exp.select("*").from_(origin.to_expr()))
                await target.load(stage, reader, "create")
            staged.add(key)
            result.transfers.append(f"{dep}: {upstream.engine} -> {model.engine} ({stage.schema}.{stage.name}, {via})")
    return local


async def _attach_transfer(
    target: EngineAdapter, uri: str | None, source_name: str, origin: TableRef, stage: TableRef
) -> bool:
    """Fast lane: when the target is DuckDB-family and the source is attachable,
    stage with one federated CTAS. Opportunistic — any failure falls back to Arrow."""
    from interlace.engines.duckdb import DuckDBAdapter
    from interlace.engines.quack import QuackAdapter

    if uri is None or not isinstance(target, DuckDBAdapter) or isinstance(target, QuackAdapter):
        return False
    alias = f"__xfer_{source_name}"
    src = exp.table_(origin.name, db=origin.schema, catalog=alias).sql(dialect="duckdb")
    dst = exp.table_(stage.name, db=stage.schema).sql(dialect="duckdb")
    try:
        target.attach(alias, uri)
        await target.execute_sql(f"CREATE OR REPLACE TABLE {dst} AS SELECT * FROM {src}")
    except Exception:
        return False  # e.g. the source file is held open by its own adapter -> Arrow path
    finally:
        # release the handle either way: the source engine must stay openable
        # by its own adapter later in this (long-lived daemon) process
        with contextlib.suppress(Exception):
            await target.execute_sql(f"DETACH {exp.to_identifier(alias).sql('duckdb')}")
    return True


async def _bootstrap_window(model: CompiledModel, resolved: exp.Query, engine: EngineAdapter) -> Interval | None:
    """The initial backfill window for a fresh incremental table: the source's
    time-column range (one aggregate scan over the resolved query), floored/
    ceiled to the model's grain and filled as ONE covering interval. ``backfill:
    <ISO date>`` pins the start instead of the derived minimum. None when the
    source holds no rows."""
    from datetime import datetime

    from interlace.state.interval import parse_grain

    grain = parse_grain(model.interval or "1d")
    column = exp.column(model.time_column or "")
    probe = exp.select(
        exp.alias_(exp.func("min", column.copy()), "lo"), exp.alias_(exp.func("max", column.copy()), "hi")
    ).from_(exp.Subquery(this=resolved.copy(), alias=exp.TableAlias(this=exp.to_identifier("src"))))
    reader = await engine.fetch(probe)
    row = reader.read_all().to_pylist()[0]
    lo, hi = row["lo"], row["hi"]
    if lo is None or hi is None:
        return None
    to_dt = lambda v: v if isinstance(v, datetime) else datetime(v.year, v.month, v.day)  # noqa: E731
    lo_dt, hi_dt = to_dt(lo), to_dt(hi)
    if model.backfill not in ("auto", "none"):
        lo_dt = datetime.fromisoformat(model.backfill)  # pinned start
    floor = datetime.min + ((lo_dt - datetime.min) // grain) * grain
    ceil = datetime.min + (-(-(hi_dt - datetime.min) // grain)) * grain
    if ceil <= floor:
        ceil = floor + grain
    return Interval(floor, ceil)


async def _seed_history(engine: EngineAdapter, source: TableRef, target: TableRef) -> None:
    """Forward-only copy-on-write: seed the new fingerprint's table from the previous
    one. Idempotent (IF NOT EXISTS), so a crashed apply re-seeds harmlessly; the old
    table is untouched and stays the rollback until gc reclaims it."""
    await engine.create_schema(target.schema)
    source_expr = source.to_expr()
    await engine.execute(
        exp.Create(
            this=target.to_expr(),
            kind="TABLE",
            exists=True,
            expression=exp.select("*").from_(source_expr),
        )
    )


async def _gate_checks(
    model: CompiledModel,
    compiled: CompiledProject,
    engine: EngineAdapter,
    state: StateStore,
    environment: str,
    result: ApplyResult,
    physical: Mapping[str, TableRef] | None = None,
    target: TableRef | None = None,
) -> None:
    """Run the model's checks against its built table; an error-severity failure raises
    before the environment is promoted. ``target`` defaults to the model's own snapshot
    table (virtual/Python); a terminal ``table`` passes its delivered external target."""
    outcomes = await run_checks(
        model, compiled, engine, target or model.physical_table, compiled.python_checks.get(model.name, ()), physical
    )
    if not outcomes:
        return
    result.checks.extend(outcomes)
    await state.record_check_results(environment, model.fingerprint, outcomes)
    blocking = [o for o in outcomes if o.blocking]
    if blocking:
        details = "; ".join(
            f"{o.model}.{o.name}: {o.message}" if o.status == "error" else f"{o.model}.{o.name} ({o.failures} failing)"
            for o in blocking
        )
        raise CheckError(f"checks failed — promotion blocked: {details}")


def _check_references(model: CompiledModel, compiled: CompiledProject) -> set[str]:
    """Other models a check reads (``relationships`` targets, tables in ``sql``
    checks): they must be built before this model's checks can run. Table refs
    are matched to models by their ``db.name`` key or bare name, the same way
    dependencies resolve — so a check reading ``raw.orders`` finds model
    ``raw.orders`` (or ``orders``), not nothing."""

    def _model_for(key: str) -> str | None:
        if key in compiled.models:
            return key
        tail = key.rsplit(".", 1)[-1]
        return tail if tail in compiled.models else None

    refs: set[str] = set()
    for spec in model.checks:
        if spec.type == "relationships":
            match = _model_for(str(spec.params.get("to", "")))
            if match:
                refs.add(match)
        elif spec.type == "sql":
            with contextlib.suppress(Exception):
                parsed = sqlglot.parse_one(str(spec.params.get("query", "")))
                for table in parsed.find_all(exp.Table):
                    match = _model_for(f"{table.db}.{table.name}" if table.db else table.name)
                    if match:
                        refs.add(match)
    return refs


async def _external_objects(
    state: StateStore, environment: str, name: str, fingerprint: str
) -> tuple[PhysicalObject, ...]:
    """Objects interlace recorded for this external table.

    The environment still points at the previous fingerprint until promote, which
    is the set to drop from. A re-delivery of the same fingerprint (``run``)
    reads that snapshot instead.
    """
    promoted = await state.get_environment(environment)
    recorded_fp = promoted.get(name) or fingerprint
    recorded = await state.get_snapshot(name, recorded_fp)
    if recorded is None and recorded_fp != fingerprint:
        recorded = await state.get_snapshot(name, fingerprint)
    return recorded.physical_objects if recorded else ()


async def _stamp_owned(
    snapshot: Snapshot,
    model: CompiledModel,
    engine: EngineAdapter,
    state: StateStore,
    notes: list[str],
) -> Snapshot:
    """Reconcile indexes and constraints on an owned snapshot table and record what was created.

    A new fingerprint is a new table, so objects on the previous table are not dropped.
    A later interval window of the same fingerprint reconciles against what that
    snapshot already recorded.
    """
    recorded = await state.get_snapshot(snapshot.name, snapshot.fingerprint)
    table_ready = await engine.table_exists(snapshot.physical_table)
    if not table_ready:
        return replace(snapshot, physical_hash=model.physical_hash)
    same = recorded is not None and recorded.physical_table == snapshot.physical_table
    previous = recorded.physical_objects if same and recorded is not None else ()
    ddl, objects, warnings = await _physical_ddl(engine, model, snapshot.physical_table, previous, same_table=same)
    for warning in warnings:
        _remember(notes, warning)
    if ddl:
        await engine.execute_all(ddl)
    return replace(snapshot, physical_hash=model.physical_hash, physical_objects=objects)


async def _run_backfill(
    task: BackfillTask,
    plan: Plan,
    compiled: CompiledProject,
    registry: EngineRegistry,
    physical: Mapping[str, TableRef],
    staged: set[tuple[str, str]],
    stage_lock: asyncio.Lock,
    state: StateStore,
    base_path: Path | None,
    result: ApplyResult,
) -> None:
    """Build one backfill task end-to-end: stage inputs, execute, contract, record, gate."""
    task_started = time.perf_counter()
    snapshot = task.snapshot
    model = compiled.models[snapshot.name]
    target_engine = registry.require(model.engine, model=model.name)
    resolution = await _stage_cross_engine_inputs(model, compiled, registry, physical, staged, stage_lock, result)

    if task.reuse_existing and await target_engine.table_exists(snapshot.physical_table):
        # fingerprint already materialised (e.g. by another environment): the content-
        # addressed table exists, so skip the (re)build compute — record the snapshot for
        # this env's promotion, gate on checks against the existing table, and let the
        # caller swap this environment's view onto the shared table. The table_exists guard
        # is what makes the differ's optimistic reuse safe: a snapshot row can outlive its
        # table (an in-memory warehouse across processes, a gc'd table) — then we fall
        # through and build for real.
        await state.add_snapshot(await _stamp_owned(snapshot, model, target_engine, state, plan.warnings))
        if snapshot.name not in result.built and snapshot.name not in result.reused:
            result.reused.append(snapshot.name)
        await _gate_checks(model, compiled, target_engine, state, plan.environment, result, resolution)
        result.timings[snapshot.name] = result.timings.get(snapshot.name, 0.0) + (time.perf_counter() - task_started)
        return

    if model.ast is None:  # Python model: run the function, load Arrow into the snapshot table
        if model.materialise != "virtual":
            raise PlanError(
                f"Python model {snapshot.name!r} must materialise as virtual; table/file (write a SQL model "
                f"over its output), view and ephemeral are not supported for Python models"
            )
        if model.strategy == "incremental" and not model.key:
            # Keyed is supported: the window bounds which staged rows are upserted.
            # Unkeyed is not, and deliberately. For a SQL model the window predicate
            # is pushed into the query so the engine only computes the window; a
            # Python function has already computed everything by the time we could
            # filter it, so an unkeyed windowed rewrite would look incremental while
            # doing the full work every run. Bound the fetch with cursor= instead.
            raise PlanError(
                f"Python model {snapshot.name!r} cannot use incremental without a key: the function "
                f"runs in full before the window can be applied, so the window would not save any work. "
                f"Add key= to upsert the window's rows, or use cursor= with merge to bound the fetch"
            )
        recorded_self = await state.get_snapshot(snapshot.name, snapshot.fingerprint)
        previous = recorded_self.physical_table if recorded_self is not None else None
        await target_engine.create_schema(snapshot.physical_table.schema)
        if task.seed_from is not None:  # forward-only: the seeded copy IS the history
            await _seed_history(target_engine, task.seed_from, snapshot.physical_table)
            previous = previous or snapshot.physical_table
        if model.strategy == "replace":
            loaded = await build_python_model(
                model, compiled, target_engine, snapshot.physical_table, physical=resolution, previous=previous
            )
            result.record_rows(snapshot.name, RowCounts(inserted=loaded))
        else:  # keyed strategy: stage the Arrow output, then merge it in SQL
            reader = await run_python_model(model, compiled, target_engine, resolution, previous)
            merged, filled_window = await _merge_python_output(
                model,
                target_engine,
                snapshot.physical_table,
                reader,
                exists=previous is not None,
                interval=task.interval,
                bootstrap=task.bootstrap,
            )
            result.record_rows(snapshot.name, merged)
            if filled_window is not None:  # incremental: accumulate the window in the ledger
                filled = await state.get_intervals(snapshot.name, snapshot.fingerprint)
                for carried in snapshot.intervals:
                    filled = filled.add(carried)
                snapshot = replace(snapshot, intervals=filled.add(filled_window))
        if model.columns:
            validate_contract(model.name, await target_engine.describe(snapshot.physical_table), model.columns)
        await state.add_snapshot(await _stamp_owned(snapshot, model, target_engine, state, plan.warnings))
        result.built.append(snapshot.name)
        await _gate_checks(model, compiled, target_engine, state, plan.environment, result, resolution)
        result.timings[snapshot.name] = time.perf_counter() - task_started
        return

    resolved = resolve_model_query(model, compiled, resolution)

    if model.is_terminal:  # deliver into an external table/file — no snapshot table, no env view
        if plan.environment not in model.environments:
            # environment-gated: a dev apply must never fire a side effect at a live
            # destination. Record the snapshot so the plan settles; deliver nothing.
            await state.add_snapshot(snapshot)
            result.gated.append(snapshot.name)
            result.timings[snapshot.name] = time.perf_counter() - task_started
            return
        if model.materialise == "file":  # overwrite a file via COPY
            export_path = _resolve_export_path(base_path, model.path or "")
            Path(export_path).parent.mkdir(parents=True, exist_ok=True)
            copied = await target_engine.execute_all(
                file_statements(model.format or "", resolved, export_path, model.dialect)
            )
            result.record_rows(snapshot.name, RowCounts(inserted=copied[0] if copied else 0))
        else:  # materialise: table — reverse ETL into an attached database via the strategy
            strategy = resolve_strategy(model.materialise, model.strategy, model.key, model.time_column)
            interval = task.interval
            if task.bootstrap:  # incremental first delivery: fill the source's whole range in one window
                interval = await _bootstrap_window(model, resolved, target_engine)
            previous_objects = await _external_objects(state, plan.environment, snapshot.name, snapshot.fingerprint)
            delivered, objects = await _deliver_table(
                model, target_engine, resolved, strategy, interval, previous_objects, plan.warnings
            )
            result.record_rows(snapshot.name, delivered)
            snapshot = replace(snapshot, physical_hash=model.physical_hash, physical_objects=objects)
            if interval is not None:  # incremental terminal: accumulate the filled window in the ledger
                filled = await state.get_intervals(snapshot.name, snapshot.fingerprint)
                for carried in snapshot.intervals:  # forward-only N/A, but keep the ledger contract uniform
                    filled = filled.add(carried)
                snapshot = replace(snapshot, intervals=filled.add(interval))
            if model.columns:  # validate the delivered external table against the contract
                validate_contract(
                    model.name, await target_engine.describe(target_ref(model.target or "")), model.columns
                )
        await state.add_snapshot(snapshot)
        if snapshot.name not in result.built:  # one entry per model, however many interval windows ran
            result.built.append(snapshot.name)
        if model.materialise == "table":  # checks run against the delivered external table (gate promotion)
            await _gate_checks(
                model,
                compiled,
                target_engine,
                state,
                plan.environment,
                result,
                resolution,
                target=target_ref(model.target or ""),
            )
        result.timings[snapshot.name] = result.timings.get(snapshot.name, 0.0) + (time.perf_counter() - task_started)
        return

    relation = SqlRelation(ast=resolved)
    strategy = resolve_strategy(model.materialise, model.strategy, model.key, model.time_column)

    await target_engine.create_schema(snapshot.physical_table.schema)
    if task.seed_from is not None:  # forward-only: history moves onto the new table first
        await _seed_history(target_engine, task.seed_from, snapshot.physical_table)
    interval = task.interval
    if task.bootstrap:  # incremental first build: fill the source's whole range in one window
        interval = await _bootstrap_window(model, resolved, target_engine)
    columns: list[str] | None = None
    if model.strategy == "merge":  # native MERGE needs the target's column list; describe it if it exists
        columns = list(await target_engine.describe(snapshot.physical_table)) or None
    statements = strategy.plan_statements(relation, snapshot.physical_table, target_engine.caps, interval, columns)
    counts = await target_engine.execute_all(statements)
    result.record_rows(snapshot.name, strategy.row_counts(counts))
    if model.columns:  # validate the built schema against the contract before recording it
        validate_contract(model.name, await target_engine.describe(snapshot.physical_table), model.columns)

    if interval is not None:  # incremental: accumulate the filled window in the ledger
        filled = await state.get_intervals(snapshot.name, snapshot.fingerprint)
        for carried in snapshot.intervals:  # forward-only: the INHERITED ledger must persist too
            filled = filled.add(carried)
        snapshot = replace(snapshot, intervals=filled.add(interval))
    await state.add_snapshot(await _stamp_owned(snapshot, model, target_engine, state, plan.warnings))
    if snapshot.name not in result.built:  # one entry per model, however many interval windows ran
        result.built.append(snapshot.name)
    await _gate_checks(model, compiled, target_engine, state, plan.environment, result, resolution)
    result.timings[snapshot.name] = result.timings.get(snapshot.name, 0.0) + (time.perf_counter() - task_started)


async def apply(
    plan: Plan,
    *,
    compiled: CompiledProject,
    engine: EngineAdapter | None = None,
    engines: Mapping[str, EngineAdapter] | EngineRegistry | None = None,
    state: StateStore,
    base_path: Path | None = None,
    parallelism: int = 4,
    on_progress: ProgressCallback | None = None,
    connections: Mapping[str, Any] | None = None,
) -> ApplyResult:
    """Execute a plan and record the result in ``state``.

    Pass either a single ``engine`` (single-engine projects / tests) or an
    ``engines`` registry / mapping. Each model builds on ``model.engine``.
    ``base_path`` is the project root used to resolve relative export paths.
    ``on_progress`` (model, event, detail) fires on the event loop as each model's
    build starts / finishes: events are ``"start"``, ``"done"``, ``"failed"``,
    ``"cancelled"``. ``detail`` carries seconds and row deltas on done, and the
    message plus the failed statement on failed. ``connections`` is bound for
    Python models, which resolve a name with :func:`interlace.connections.connection`.
    Models registered while a Python model runs are listed on ``result.registered``;
    the caller compiles and builds them.
    """
    registry = as_registry(engine, engines)
    for adapter in registry.opened():
        refresh = getattr(adapter, "refresh_inputs", None)
        if refresh is not None:
            refresh()
    from interlace.physical.annotate import annotate_plan

    await annotate_plan(plan, compiled, registry)
    if plan.blocking:
        raise PlanError("schema drift blocks apply: " + "; ".join(plan.blocking))
    result = ApplyResult()

    # Where each model's data actually lives: recorded snapshots win over the
    # fingerprint-derived name (a reused snapshot sits on an older table), and
    # models building in this apply resolve to where they are being built now.
    recorded_snapshots = await state.get_snapshots(
        (name, compiled_model.fingerprint) for name, compiled_model in compiled.models.items()
    )
    physical: dict[str, TableRef] = {
        name: snapshot.physical_table for (name, _), snapshot in recorded_snapshots.items()
    }
    for task in plan.backfills:
        physical[task.snapshot.name] = task.snapshot.physical_table
    for reuse in plan.reuses:
        physical[reuse.name] = reuse.physical_table

    # True DAG scheduling: every model starts the moment its last in-plan
    # ancestor finishes — no level barriers, so one slow branch never stalls an
    # independent one; wall-clock tracks the critical path. Tasks still group
    # per model (a model's interval windows stay ordered) and a semaphore bounds
    # concurrency. Waiting happens BEFORE the semaphore, so a blocked model
    # never holds a build slot its own upstream needs.
    staged: set[tuple[str, str]] = set()  # (upstream, target engine) pairs moved this apply
    stage_lock = asyncio.Lock()
    per_model: dict[str, list[BackfillTask]] = {}
    for task in plan.backfills:  # differ/run emit tasks in topological order
        per_model.setdefault(task.snapshot.name, []).append(task)
    # Blocking edges project the dependency graph onto the models building now,
    # walking THROUGH models that aren't (ephemeral, reused): a Python model over
    # an ephemeral view of a building table must still wait for that table.
    data_deps = {model.name: set(model.dependencies) - {model.name} for model in compiled.models.values()}

    def in_plan_ancestors(name: str) -> set[str]:
        """The building models this model must wait for, following DATA edges only
        (through non-building intermediaries). Data edges are acyclic, so this is
        always safe to enforce."""
        found: set[str] = set()
        seen = {name}
        stack = list(data_deps.get(name, ()))
        while stack:
            dep = stack.pop()
            if dep in seen:
                continue
            seen.add(dep)
            if dep in per_model:
                found.add(dep)
            else:  # not building: look through it
                stack.extend(data_deps.get(dep, ()))
        return found

    # Checks that read *other* models (relationships, sql) add scheduling edges the
    # data DAG doesn't have. Keep one unless it would close a cycle — which it does
    # exactly when the reader is already an ancestor of what its check reads (a check
    # pointing downstream, e.g. order_items testing against orders, which is built
    # from order_items). Those are dropped: the check runs before its target exists
    # and fails, but a cycle would hang the whole apply, and the model can point its
    # check at an upstream instead. Everything else is a plain sibling reference and
    # is enforced — dropping those on a topological-order heuristic failed checks
    # whose target simply happened to sort later.
    blocking: dict[str, set[str]] = {}
    for name in per_model:
        deps = in_plan_ancestors(name)
        for ref in _check_references(compiled.models[name], compiled):
            if ref in per_model and name not in compiled.graph.ancestors(ref):
                deps.add(ref)
        blocking[name] = deps

    finished = {name: asyncio.Event() for name in per_model}
    build_slots = asyncio.Semaphore(max(1, parallelism))

    async def run_model(name: str) -> None:
        try:
            for dep in blocking[name]:
                await finished[dep].wait()
            async with build_slots:
                if on_progress is not None:
                    on_progress(name, "start", {})
                for model_task in per_model[name]:
                    await _run_backfill(
                        model_task, plan, compiled, registry, physical, staged, stage_lock, state, base_path, result
                    )
        except asyncio.CancelledError:  # a SIBLING failed; this model is collateral
            if on_progress is not None:
                on_progress(name, "cancelled", {})
            raise
        except BaseException as exc:
            # Name the failing model as live feedback; the full message is surfaced once
            # by the caller (the CLI prints it, the API returns it) — don't duplicate it here.
            logger.warning("model %s failed (%s)", name, type(exc).__name__)
            detail = _failure_detail(exc)
            if on_progress is not None:
                on_progress(name, "failed", detail)
            # Wrap a plain build error (engine/SQL/Python-model exception) so it reads as one
            # clean "error: model … failed: …" line, not a raw traceback. InterlaceErrors
            # (checks, contracts) already carry a good message; other BaseExceptions
            # (KeyboardInterrupt, CancelledError) must propagate untouched.
            if isinstance(exc, Exception) and not isinstance(exc, InterlaceError):
                wrapped = {"model": name}
                if "statement" in detail:
                    wrapped["statement"] = detail["statement"]
                raise ExecutionError(f"model {name!r} failed: {detail['message']}", details=wrapped) from exc
            if isinstance(exc, InterlaceError) and "statement" in detail:
                exc.details.setdefault("statement", detail["statement"])
            raise
        finished[name].set()
        if on_progress is not None:
            on_progress(name, "done", _build_detail(result, name))

    from interlace.connections import bind_connections, unbind_connections

    # Copied into each build task at creation, so a Python model that registers
    # models (including from a worker thread) appends to this same list.
    with capturing_registrations() as batch:
        bound = bind_connections(connections)
        try:
            try:
                async with asyncio.TaskGroup() as group:
                    for name in per_model:
                        group.create_task(run_model(name))
            except ExceptionGroup as failures:  # single failure keeps apply()'s plain-exception contract
                if len(failures.exceptions) == 1:
                    # re-raise the plain single exception, preserving its own cause (the build error,
                    # kept for --debug) rather than re-chaining the ExceptionGroup
                    failure = failures.exceptions[0]
                    raise failure from failure.__cause__
                raise
        finally:
            unbind_connections(bound)
    result.registered = list(batch)

    for reuse in plan.reuses:  # output provably identical: record the fingerprint, build nothing
        await state.add_snapshot(reuse)
        result.reused.append(reuse.name)

    built_now = {task.snapshot.name for task in plan.backfills}
    for action in plan.physical:
        if not action.standalone or action.name in built_now:
            continue
        model = compiled.models[action.name]
        if model.materialise == "file":
            continue
        target_engine = registry.require(model.engine, model=model.name)
        if model.is_terminal:
            if plan.environment not in model.environments or not model.target:
                continue
            table = target_ref(model.target)
        else:
            recorded = await state.get_snapshot(model.name, model.fingerprint)
            table = recorded.physical_table if recorded is not None else model.physical_table
        if not await target_engine.table_exists(table):
            continue
        ddl, objects, warnings = await _physical_ddl(target_engine, model, table, action.previous, same_table=True)
        for warning in warnings:
            _remember(plan.warnings, warning)
        if ddl:
            await target_engine.execute_all(ddl)
        recorded = await state.get_snapshot(model.name, model.fingerprint)
        if recorded is None:
            continue
        await state.add_snapshot(replace(recorded, physical_hash=model.physical_hash, physical_objects=objects))

    ensured: set[tuple[str, str]] = set()  # (engine, schema): one CREATE SCHEMA per pair, not per view
    for swap in plan.virtual_updates:
        view_engine = registry.require(swap.engine)
        if (swap.engine, swap.view.schema) not in ensured:
            await view_engine.create_schema(swap.view.schema)
            ensured.add((swap.engine, swap.view.schema))
        await view_engine.create_view(swap.view, swap.target)

    mapping = {name: compiled.models[name].fingerprint for name in plan.promote}
    await state.promote(plan.environment, mapping)
    # ephemeral models are tracked in the mapping (so re-plans stay clean) but are inlined
    # into consumers — they have no promotable table/view, so the user-facing count omits
    # them, keeping "promoted N" consistent with the N build rows shown
    result.promoted = sum(1 for name in mapping if compiled.models[name].materialise != "ephemeral")

    # deleted models: drop their env view and demote them, or the view serves the
    # last snapshot forever and pins it against gc
    removed = [c for c in plan.changes if c.change_type is ChangeType.REMOVED]
    if removed:
        last_snapshots = await state.get_snapshots(
            (c.name, c.previous_fingerprint) for c in removed if c.previous_fingerprint is not None
        )
        for change in removed:
            snapshot = last_snapshots.get((change.name, change.previous_fingerprint or ""))
            view = env_view(plan.environment, change.name)
            with contextlib.suppress(Exception):
                # best effort: the model's engine may have been deleted from config
                # along with the model — the DEMOTE below must still happen, or the
                # removal never settles and every later apply fails right here
                adapter = registry.require(snapshot.engine if snapshot is not None else registry.default)
                await adapter.execute(drop(view, kind="VIEW"))
        await state.demote(plan.environment, [c.name for c in removed])
    return result
