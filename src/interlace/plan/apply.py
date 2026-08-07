"""Apply a plan: build changed snapshots, repoint environment views, promote.

For each backfill the model's query has its upstream references rewritten to the
upstreams' physical tables, the strategy emits the build statements, and the
engine runs them; the new snapshot is persisted. Then the environment's virtual
views are repointed at the new physical tables, and the environment is promoted
to the full desired fingerprint set. Builds are DAG-scheduled: each model starts
as soon as its in-plan ancestors finish (bounded by ``parallelism``), so upstream
physical tables always exist before a downstream model builds against them.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path

import pyarrow as pa
import sqlglot
from sqlglot import exp

from interlace.checks.runner import CheckOutcome, run_checks
from interlace.contracts import validate_contract
from interlace.engines.base import EngineAdapter
from interlace.engines.registry import EngineRegistry, as_registry
from interlace.exceptions import CheckError, ExecutionError, InterlaceError, PlanError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.relation import SqlRelation, TableRef
from interlace.plan.plan import XFER_SCHEMA, BackfillTask, ChangeType, Plan, env_view, staging_table
from interlace.plan.resolve import resolve_model_query
from interlace.runtime.python_model import build_python_model, run_python_model
from interlace.sinks import file_statements, target_ref
from interlace.state.interval import Interval
from interlace.state.store import StateStore
from interlace.strategies import Strategy, resolve_strategy
from interlace.strategies.base import RowCounts


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


def _resolve_export_path(base_path: Path | None, path: str) -> str:
    target = Path(path)
    if target.is_absolute():
        return str(target)
    return str((base_path or Path.cwd()) / target)


async def _merge_python_output(
    model: CompiledModel,
    engine: EngineAdapter,
    target: TableRef,
    reader: pa.RecordBatchReader,
    *,
    exists: bool,
) -> RowCounts:
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
    pre_statements: list[exp.Expression] = []
    columns: list[str] | None = None
    if exists:
        pre_statements, source, columns = await _align_stage_to_target(
            engine, stage, target, exclude=strategy.managed_columns
        )

    relation = SqlRelation(ast=source)
    statements = strategy.plan_statements(relation, target, engine.caps, None, columns)
    drop_stage = exp.Drop(this=stage_table.copy(), kind="TABLE", exists=True)
    counts = await engine.execute_all([*pre_statements, *statements, drop_stage])
    return strategy.row_counts(counts[len(pre_statements) : len(pre_statements) + len(statements)])


async def _align_stage_to_target(
    engine: EngineAdapter, stage: TableRef, target: TableRef, exclude: tuple[str, ...] = ()
) -> tuple[list[exp.Expression], exp.Query, list[str]]:
    """Align a staged source to an EXISTING target: additive ALTERs for new columns,
    widening type promotions in place, and a projection over the stage matching the
    target's final column set (NULL-fill vanished columns, cast type drift). Returns
    (pre-statements, aligned source select, target column order).

    ``exclude`` names strategy-managed bookkeeping columns (e.g. scd2's validity
    pair): they live on the target but never in the model's output, so they must
    not be NULL-filled into the aligned source — the strategy owns them."""
    target_columns = await engine.describe(target)
    for column in exclude:
        target_columns.pop(column, None)
    stage_columns = await engine.describe(stage)
    target_expr = target.to_expr()
    pre_statements: list[exp.Expression] = []
    for column, dtype in stage_columns.items():
        if column not in target_columns:
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
    projection = []
    for column, dtype in target_columns.items():
        if column not in stage_columns:
            projection.append(exp.alias_(exp.Cast(this=exp.Null(), to=exp.DataType.build(dtype)), column))
        elif stage_columns[column] != dtype:
            projection.append(exp.alias_(exp.Cast(this=exp.column(column), to=exp.DataType.build(dtype)), column))
        else:
            projection.append(exp.column(column))
    source = exp.select(*projection).from_(stage.to_expr())
    return pre_statements, source, list(target_columns)


async def _deliver_table(
    model: CompiledModel,
    engine: EngineAdapter,
    resolved: exp.Expression,
    strategy: Strategy,
    interval: Interval | None,
) -> RowCounts:
    """Deliver ``resolved`` into an external table (``materialise: table``) via
    ``strategy`` (replace / append / merge / full_merge / incremental_by_time).

    The external target is never dropped (grants and readers survive). When it already
    exists the source is staged in the warehouse and aligned to the target (additive
    ALTERs, widening, NULL-fill, casts) so a model that grows or reorders columns evolves
    the destination instead of breaking it or positionally corrupting it. The insert
    binds positionally against the aligned source, which reproduces the target's column
    order exactly.

    Two cases skip staging and run the strategy directly against the target: the first
    delivery (the ensure-create matches the source), and any windowed ``incremental_by_time``
    delivery (``interval`` set). An incremental window is grain-scoped and stays
    schema-stable within a fingerprint, so staging the *whole* source once per window
    would make a wide backfill/restate O(windows × source) — the pathological case."""
    target = target_ref(model.target or "")
    if interval is not None or not await engine.table_exists(target):
        counts = await engine.execute_all(
            strategy.plan_statements(SqlRelation(ast=resolved), target, engine.caps, interval)
        )
        return strategy.row_counts(counts)
    stage = TableRef(schema=XFER_SCHEMA, name=f"{model.name}__sink_stage")
    await engine.create_schema(stage.schema)
    await engine.execute(exp.Create(this=stage.to_expr(), kind="TABLE", replace=True, expression=resolved.copy()))
    pre_statements, aligned, columns = await _align_stage_to_target(
        engine, stage, target, exclude=strategy.managed_columns
    )
    statements = strategy.plan_statements(SqlRelation(ast=aligned), target, engine.caps, interval, columns)
    # One transaction may write only ONE attached database: the delivery batch writes the
    # external target; the stage lives in the warehouse and is dropped separately (a
    # leftover is harmless — the next delivery CREATE OR REPLACEs it).
    counts = await engine.execute_all([*pre_statements, *statements])
    await engine.execute(exp.Drop(this=stage.to_expr(), kind="TABLE", exists=True))
    return strategy.row_counts(counts[len(pre_statements) :])


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


async def _bootstrap_window(model: CompiledModel, resolved: exp.Expression, engine: EngineAdapter) -> Interval | None:
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

    if model.ast is None:  # Python model: run the function, load Arrow into the snapshot table
        if model.materialise != "virtual":
            raise PlanError(
                f"Python model {snapshot.name!r} must materialise as virtual; table/file (write a SQL model "
                f"over its output), view and ephemeral are not supported for Python models"
            )
        if model.strategy == "incremental_by_time":
            raise PlanError(
                f"Python model {snapshot.name!r} cannot use incremental_by_time; " f"use cursor= with merge instead"
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
            merged = await _merge_python_output(
                model, target_engine, snapshot.physical_table, reader, exists=previous is not None
            )
            result.record_rows(snapshot.name, merged)
        if model.columns:
            validate_contract(model.name, await target_engine.describe(snapshot.physical_table), model.columns)
        await state.add_snapshot(snapshot)
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
            result.record_rows(snapshot.name, await _deliver_table(model, target_engine, resolved, strategy, interval))
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
    await state.add_snapshot(snapshot)
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
    on_progress: Callable[[str, str], None] | None = None,
) -> ApplyResult:
    """Execute a plan and record the result in ``state``.

    Pass either a single ``engine`` (single-engine projects / tests) or an
    ``engines`` registry / mapping. Each model builds on ``model.engine``.
    ``base_path`` is the project root used to resolve relative export paths.
    ``on_progress`` (model, event) fires on the event loop as each model's
    build starts / finishes: events are ``"start"``, ``"done"``, ``"failed"``.
    """
    registry = as_registry(engine, engines)
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
    # data DAG doesn't have — and may point "backwards", which would deadlock the
    # per-model events. Keep a check edge only when it agrees with the topological
    # order (dep built before the reader); a back edge is dropped (its check may run
    # a beat early, far better than a hang). Data edges always agree, so they all stay.
    topo = {name: index for index, name in enumerate(compiled.graph.topological_sort())}
    blocking: dict[str, set[str]] = {}
    for name in per_model:
        deps = in_plan_ancestors(name)
        for ref in _check_references(compiled.models[name], compiled):
            if ref in per_model and topo.get(ref, -1) < topo.get(name, 0):
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
                    on_progress(name, "start")
                for model_task in per_model[name]:
                    await _run_backfill(
                        model_task, plan, compiled, registry, physical, staged, stage_lock, state, base_path, result
                    )
        except asyncio.CancelledError:  # a SIBLING failed; this model is collateral
            if on_progress is not None:
                on_progress(name, "cancelled")
            raise
        except BaseException as exc:
            # Name the failing model as live feedback; the full message is surfaced once
            # by the caller (the CLI prints it, the API returns it) — don't duplicate it here.
            logger.warning("model %s failed (%s)", name, type(exc).__name__)
            if on_progress is not None:
                on_progress(name, "failed")
            # Wrap a plain build error (engine/SQL/Python-model exception) so it reads as one
            # clean "error: model … failed: …" line, not a raw traceback. InterlaceErrors
            # (checks, contracts) already carry a good message; other BaseExceptions
            # (KeyboardInterrupt, CancelledError) must propagate untouched.
            if isinstance(exc, Exception) and not isinstance(exc, InterlaceError):
                message = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
                raise ExecutionError(f"model {name!r} failed: {message}", details={"model": name}) from exc
            raise
        finished[name].set()
        if on_progress is not None:
            on_progress(name, "done")

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

    for reuse in plan.reuses:  # output provably identical: record the fingerprint, build nothing
        await state.add_snapshot(reuse)
        result.reused.append(reuse.name)

    ensured: set[tuple[str, str]] = set()  # (engine, schema): one CREATE SCHEMA per pair, not per view
    for swap in plan.virtual_updates:
        view_engine = registry.require(swap.engine)
        if (swap.engine, swap.view.schema) not in ensured:
            await view_engine.create_schema(swap.view.schema)
            ensured.add((swap.engine, swap.view.schema))
        await view_engine.create_view(swap.view, swap.target)

    mapping = {name: compiled.models[name].fingerprint for name in plan.promote}
    await state.promote(plan.environment, mapping)
    result.promoted = len(mapping)

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
                await adapter.execute(exp.Drop(this=exp.table_(view.name, db=view.schema), kind="VIEW", exists=True))
        await state.demote(plan.environment, [c.name for c in removed])
    return result
