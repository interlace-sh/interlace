"""Build one backfill task: Python, SQL snapshot, or a terminal delivery.

Promotion stays in ``apply``. This module stops when the snapshot is recorded
and its checks have gated.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
from sqlglot import exp

from interlace.checks.runner import run_checks
from interlace.contracts import validate_contract
from interlace.engines.base import EngineAdapter
from interlace.engines.registry import EngineRegistry
from interlace.exceptions import CheckError, PlanError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.relation import SqlRelation, TableRef, drop
from interlace.physical.spec import PhysicalObject
from interlace.plan.delivery import deliver_table, open_external_delivery, physical_ddl
from interlace.plan.files import drop_file_stages, stage_file_scans
from interlace.plan.fit import align_stage_to_target, remember
from interlace.plan.plan import BackfillTask, Plan
from interlace.plan.resolve import resolve_model_query
from interlace.plan.result import ApplyResult, record_build, record_timing
from interlace.plan.transfer import stage_cross_engine_inputs
from interlace.runtime.python_model import build_python_model, run_python_model
from interlace.sinks import file_statements, write_arrow_file
from interlace.state.interval import Interval
from interlace.state.snapshot import Snapshot
from interlace.state.store import StateStore
from interlace.strategies import Strategy, resolve_strategy
from interlace.strategies.base import RowCounts


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
        alignment = await align_stage_to_target(engine, stage, target, strategy)
        pre_statements = alignment.pre_statements
        source, columns = alignment.for_strategy(strategy)
    elif strategy.writes_named_columns and strategy.managed_columns:
        # The strategy builds bookkeeping columns from the model's own list. On the
        # first build the target doesn't exist yet (so no align pass ran).
        columns = [c for c in await engine.describe(stage) if c not in strategy.managed_columns]

    relation = SqlRelation(ast=source)
    if strategy.requires_interval and bootstrap:
        # First build of an incremental Python model: the range comes from the
        # staged output, since there is no query to probe the way a SQL model has.
        interval = await _bootstrap_window(model, exp.select("*").from_(stage_table.copy()), engine)
    planned = strategy.plan_statements(relation, target, engine.caps, interval, columns)
    drop_stage = drop(stage_table.copy(), kind="TABLE")
    counts = await engine.execute_all([*pre_statements, *planned, drop_stage])
    written = planned.row_counts(counts[len(pre_statements) : len(pre_statements) + len(planned)])
    return written, interval


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
    ddl, objects, warnings = await physical_ddl(engine, model, snapshot.physical_table, previous, same_table=same)
    for warning in warnings:
        remember(notes, warning)
    if ddl:
        await engine.execute_all(ddl)
    return replace(snapshot, physical_hash=model.physical_hash, physical_objects=objects)


async def _named_columns(strategy: Strategy, engine: EngineAdapter, table: TableRef) -> list[str] | None:
    """Target columns for a strategy that names its writes, excluding bookkeeping columns.

    None when the strategy binds positionally, or the target does not exist yet
    (a first build, where the ensure-create matches the source)."""
    if not strategy.writes_named_columns:
        return None
    described = await engine.describe(table)
    if not described:
        return None
    managed = {column.casefold() for column in strategy.managed_columns}
    names = [name for name in described if name.casefold() not in managed]
    return names or None


async def _accumulate_interval(state: StateStore, snapshot: Snapshot, interval: Interval | None) -> Snapshot:
    """Fold ``interval`` into the ledger, keeping intervals a forward-only seed carried in."""
    if interval is None:
        return snapshot
    filled = await state.get_intervals(snapshot.name, snapshot.fingerprint)
    for carried in snapshot.intervals:
        filled = filled.add(carried)
    return replace(snapshot, intervals=filled.add(interval))


async def _deliver_terminal(
    model: CompiledModel,
    target_engine: EngineAdapter,
    resolved: exp.Query,
    task: BackfillTask,
    plan: Plan,
    compiled: CompiledProject,
    registry: EngineRegistry,
    state: StateStore,
    base_path: Path | None,
    result: ApplyResult,
    resolution: Mapping[str, TableRef],
    snapshot: Snapshot,
    task_started: float,
) -> None:
    """Deliver a terminal model: a host file, or a table in an attached database."""
    if model.materialise == "file":
        export_path = _resolve_export_path(base_path, model.path or "")
        Path(export_path).parent.mkdir(parents=True, exist_ok=True)
        if target_engine.dialect == "duckdb":
            copied = await target_engine.execute_all(
                file_statements(model.format or "", resolved, export_path, model.dialect)
            )
            inserted = copied[0] if copied else 0
        else:
            exported = (await target_engine.fetch(resolved)).read_all()
            write_arrow_file(model.format or "", exported, export_path)
            inserted = exported.num_rows
        result.record_rows(snapshot.name, RowCounts(inserted=inserted))
        await state.add_snapshot(snapshot)
        record_build(result, snapshot.name, task_started)
        return

    strategy = resolve_strategy(model.materialise, model.strategy, model.key, model.time_column)
    interval = task.interval
    if task.bootstrap:  # incremental first delivery: fill the source's whole range in one window
        interval = await _bootstrap_window(model, resolved, target_engine)
    previous_objects = await _external_objects(state, plan.environment, snapshot.name, snapshot.fingerprint)
    delivery = await open_external_delivery(model, target_engine, resolved, registry.sinks)
    try:
        delivered, objects = await deliver_table(
            model,
            delivery.engine,
            delivery.query,
            strategy,
            interval,
            previous_objects,
            plan.warnings,
            destination=delivery.table,
        )
        result.record_rows(snapshot.name, delivered)
        snapshot = replace(snapshot, physical_hash=model.physical_hash, physical_objects=objects)
        snapshot = await _accumulate_interval(state, snapshot, interval)
        if model.columns:  # validate the delivered external table against the contract
            validate_contract(model.name, await delivery.engine.describe(delivery.table), model.columns)
        await state.add_snapshot(snapshot)
        await _gate_checks(
            model, compiled, delivery.engine, state, plan.environment, result, resolution, target=delivery.table
        )
        record_build(result, snapshot.name, task_started)
    finally:
        await delivery.close()


async def run_backfill(  # noqa: C901
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
    resolution = await stage_cross_engine_inputs(model, compiled, registry, physical, staged, stage_lock, result)

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
        record_timing(result, snapshot.name, task_started)
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
            snapshot = await _accumulate_interval(state, snapshot, filled_window)
        if model.columns:
            validate_contract(model.name, await target_engine.describe(snapshot.physical_table), model.columns)
        await state.add_snapshot(await _stamp_owned(snapshot, model, target_engine, state, plan.warnings))
        await _gate_checks(model, compiled, target_engine, state, plan.environment, result, resolution)
        record_build(result, snapshot.name, task_started)
        return

    resolved = resolve_model_query(model, compiled, resolution)
    if model.is_terminal and plan.environment not in model.environments:
        # environment-gated: a dev apply must never fire a side effect at a live
        # destination. Record the snapshot so the plan settles; deliver nothing.
        await state.add_snapshot(snapshot)
        result.gated.append(snapshot.name)
        record_timing(result, snapshot.name, task_started)
        return

    resolved, file_stages = await stage_file_scans(target_engine, resolved, base_path or Path.cwd(), model.name)
    try:
        if model.is_terminal:  # deliver into an external table/file — no snapshot table, no env view
            await _deliver_terminal(
                model,
                target_engine,
                resolved,
                task,
                plan,
                compiled,
                registry,
                state,
                base_path,
                result,
                resolution,
                snapshot,
                task_started,
            )
            return

        relation = SqlRelation(ast=resolved)
        strategy = resolve_strategy(model.materialise, model.strategy, model.key, model.time_column)

        await target_engine.create_schema(snapshot.physical_table.schema)
        if task.seed_from is not None:  # forward-only: history moves onto the new table first
            await _seed_history(target_engine, task.seed_from, snapshot.physical_table)
        interval = task.interval
        if task.bootstrap:  # incremental first build: fill the source's whole range in one window
            interval = await _bootstrap_window(model, resolved, target_engine)
        columns = await _named_columns(strategy, target_engine, snapshot.physical_table)
        planned = strategy.plan_statements(relation, snapshot.physical_table, target_engine.caps, interval, columns)
        counts = await target_engine.execute_all(planned)
        result.record_rows(snapshot.name, planned.row_counts(counts))
        if model.columns:  # validate the built schema against the contract before recording it
            validate_contract(model.name, await target_engine.describe(snapshot.physical_table), model.columns)

        snapshot = await _accumulate_interval(state, snapshot, interval)
        await state.add_snapshot(await _stamp_owned(snapshot, model, target_engine, state, plan.warnings))
        await _gate_checks(model, compiled, target_engine, state, plan.environment, result, resolution)
        record_build(result, snapshot.name, task_started)
    finally:
        await drop_file_stages(target_engine, file_stages)
