"""Apply a plan: build changed snapshots, repoint environment views, promote.

For each backfill the model's query has its upstream references rewritten to the
upstreams' physical tables, the strategy emits the build statements, and the
engine runs them; the new snapshot is persisted. Then the environment's virtual
views are repointed at the new physical tables, and the environment is promoted
to the full desired fingerprint set. Runs in the compiled topological order, so
upstream physical tables exist before downstream models build against them.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path

import pyarrow as pa
from sqlglot import exp

from interlace.checks.runner import CheckOutcome, run_checks
from interlace.contracts import validate_contract
from interlace.engines.base import EngineAdapter
from interlace.engines.registry import EngineRegistry, as_registry
from interlace.exceptions import CheckError, PlanError
from interlace.exports import export_statements, table_export_statements
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.relation import EngineRef, SqlRelation, TableRef
from interlace.ir.schema import empty_schema
from interlace.plan.plan import Plan
from interlace.plan.resolve import resolve_model_query
from interlace.runtime.python_model import build_python_model, run_python_model
from interlace.state.store import StateStore
from interlace.strategies import resolve_strategy


@dataclass
class ApplyResult:
    built: list[str] = field(default_factory=list)
    reused: list[str] = field(default_factory=list)  # recorded over their previous physical table
    promoted: int = 0
    checks: list[CheckOutcome] = field(default_factory=list)
    # Wall-clock build seconds per built model (extraction + strategy + checks).
    timings: dict[str, float] = field(default_factory=dict)


# The widening promotions DuckLake's ALTER COLUMN supports, in order.
_NUMERIC_WIDTH = {"TINYINT": 0, "SMALLINT": 1, "INTEGER": 2, "BIGINT": 3, "FLOAT": 4, "DOUBLE": 5}


def _widens(current: str, incoming: str) -> bool:
    """True when ``incoming`` is a strictly wider numeric type than ``current``."""
    return (
        current in _NUMERIC_WIDTH and incoming in _NUMERIC_WIDTH and _NUMERIC_WIDTH[incoming] > _NUMERIC_WIDTH[current]
    )


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
) -> None:
    """Stage a Python model's Arrow output and apply its keyed strategy in SQL.

    The output lands in a stage table (CREATE OR REPLACE, so a crashed run's
    leftover is harmless), the target is evolved additively when the output grew
    new columns, and the strategy's statements run atomically against a source
    select aligned to the target's column set. The stage is dropped in the same
    batch.
    """
    stage = replace(target, name=f"{target.name}__stage")
    await engine.load(stage, reader, "create")
    stage_table = exp.table_(stage.name, db=stage.schema, catalog=stage.catalog)

    source: exp.Query = exp.select("*").from_(stage_table.copy())
    pre_statements: list[exp.Expression] = []
    if exists:  # align the stage to the target: new columns, NULL-fill vanished, type drift
        target_columns = await engine.describe(target)
        stage_columns = await engine.describe(stage)
        target_expr = exp.table_(target.name, db=target.schema, catalog=target.catalog)
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
        source = exp.select(*projection).from_(stage_table.copy())

    relation = SqlRelation(
        ast=source, engine=EngineRef(name=model.engine, dialect=model.dialect), schema=empty_schema()
    )
    strategy = resolve_strategy(model.materialise, model.strategy, model.key, model.time_column)
    statements = strategy.plan_statements(relation, target, engine.caps, None)
    drop_stage = exp.Drop(this=stage_table.copy(), kind="TABLE", exists=True)
    await engine.execute_all([*pre_statements, *statements, drop_stage])


async def _gate_checks(
    model: CompiledModel,
    compiled: CompiledProject,
    engine: EngineAdapter,
    state: StateStore,
    environment: str,
    result: ApplyResult,
    physical: Mapping[str, TableRef] | None = None,
) -> None:
    """Run the model's checks against its built snapshot table; an error-severity
    failure raises before the environment is promoted."""
    outcomes = await run_checks(
        model, compiled, engine, model.physical_table, compiled.python_checks.get(model.name, ()), physical
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


async def apply(
    plan: Plan,
    *,
    compiled: CompiledProject,
    engine: EngineAdapter | None = None,
    engines: Mapping[str, EngineAdapter] | EngineRegistry | None = None,
    state: StateStore,
    base_path: Path | None = None,
) -> ApplyResult:
    """Execute a plan and record the result in ``state``.

    Pass either a single ``engine`` (single-engine projects / tests) or an
    ``engines`` registry / mapping. Each model builds on ``model.engine``.
    ``base_path`` is the project root used to resolve relative export paths.
    """
    registry = as_registry(engine, engines)
    result = ApplyResult()

    # Where each model's data actually lives: recorded snapshots win over the
    # fingerprint-derived name (a reused snapshot sits on an older table), and
    # models building in this apply resolve to where they are being built now.
    physical: dict[str, TableRef] = {}
    for name, compiled_model in compiled.models.items():
        recorded = await state.get_snapshot(name, compiled_model.fingerprint)
        if recorded is not None:
            physical[name] = recorded.physical_table
    for task in plan.backfills:
        physical[task.snapshot.name] = task.snapshot.physical_table
    for reuse in plan.reuses:
        physical[reuse.name] = reuse.physical_table

    for task in plan.backfills:
        task_started = time.perf_counter()
        snapshot = task.snapshot
        model = compiled.models[snapshot.name]
        target_engine = registry.require(model.engine, model=model.name)

        if model.ast is None:  # Python model: run the function, load Arrow into the snapshot table
            if model.export is not None:
                raise PlanError(f"Python model {snapshot.name!r} cannot be a sink yet; write SQL over its output")
            if model.materialise != "table":
                raise PlanError(f"Python model {snapshot.name!r} must materialise as a table")
            if model.strategy == "incremental_by_time":
                raise PlanError(
                    f"Python model {snapshot.name!r} cannot use incremental_by_time; "
                    f"use cursor= with merge_by_key instead"
                )
            recorded_self = await state.get_snapshot(snapshot.name, snapshot.fingerprint)
            previous = recorded_self.physical_table if recorded_self is not None else None
            await target_engine.create_schema(snapshot.physical_table.schema)
            if model.strategy == "full":
                await build_python_model(
                    model, compiled, target_engine, snapshot.physical_table, physical=physical, previous=previous
                )
            else:  # keyed strategy: stage the Arrow output, then merge it in SQL
                reader = await run_python_model(model, compiled, target_engine, physical, previous)
                await _merge_python_output(
                    model, target_engine, snapshot.physical_table, reader, exists=previous is not None
                )
            if model.columns:
                validate_contract(model.name, await target_engine.describe(snapshot.physical_table), model.columns)
            await state.add_snapshot(snapshot)
            result.built.append(snapshot.name)
            await _gate_checks(model, compiled, target_engine, state, plan.environment, result, physical)
            result.timings[snapshot.name] = time.perf_counter() - task_started
            continue

        resolved = resolve_model_query(model, compiled, physical)

        if model.export is not None:  # sink: push the result to a destination, no table/view
            if model.export.to == "table":  # reverse ETL into an attached database
                await target_engine.execute_all(
                    table_export_statements(model.export, resolved, model.dialect, model.engine)
                )
            else:
                export_path = _resolve_export_path(base_path, model.export.path)
                Path(export_path).parent.mkdir(parents=True, exist_ok=True)
                await target_engine.execute_all(export_statements(model.export, resolved, export_path, model.dialect))
            await state.add_snapshot(snapshot)
            result.built.append(snapshot.name)
            result.timings[snapshot.name] = time.perf_counter() - task_started
            continue

        relation = SqlRelation(
            ast=resolved, engine=EngineRef(name=model.engine, dialect=model.dialect), schema=empty_schema()
        )
        strategy = resolve_strategy(model.materialise, model.strategy, model.key, model.time_column)

        await target_engine.create_schema(snapshot.physical_table.schema)
        statements = strategy.plan_statements(relation, snapshot.physical_table, target_engine.caps, task.interval)
        await target_engine.execute_all(statements)
        if model.columns:  # validate the built schema against the contract before recording it
            validate_contract(model.name, await target_engine.describe(snapshot.physical_table), model.columns)

        if task.interval is not None:  # incremental: accumulate the filled window in the ledger
            filled = (await state.get_intervals(snapshot.name, snapshot.fingerprint)).add(task.interval)
            snapshot = replace(snapshot, intervals=filled)
        await state.add_snapshot(snapshot)
        result.built.append(snapshot.name)
        await _gate_checks(model, compiled, target_engine, state, plan.environment, result, physical)
        result.timings[snapshot.name] = time.perf_counter() - task_started

    for reuse in plan.reuses:  # output provably identical: record the fingerprint, build nothing
        await state.add_snapshot(reuse)
        result.reused.append(reuse.name)

    for swap in plan.virtual_updates:
        view_engine = registry.require(swap.engine)
        await view_engine.create_schema(swap.view.schema)
        await view_engine.create_view(swap.view, swap.target)

    mapping = {name: compiled.models[name].fingerprint for name in plan.promote}
    await state.promote(plan.environment, mapping)
    result.promoted = len(mapping)
    return result
