"""Apply a plan: build changed snapshots, repoint environment views, promote.

For each backfill the model's query has its upstream references rewritten to the
upstreams' physical tables, the strategy emits the build statements, and the
engine runs them; the new snapshot is persisted. Then the environment's virtual
views are repointed at the new physical tables, and the environment is promoted
to the full desired fingerprint set. Runs in the compiled topological order, so
upstream physical tables exist before downstream models build against them.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from interlace.contracts import validate_contract
from interlace.engines.base import EngineAdapter
from interlace.exceptions import PlanError
from interlace.graph.project import CompiledProject
from interlace.ir.relation import EngineRef, SqlRelation
from interlace.ir.schema import empty_schema
from interlace.plan.plan import Plan
from interlace.plan.resolve import resolve_model_query
from interlace.state.store import StateStore
from interlace.strategies import resolve_strategy


@dataclass
class ApplyResult:
    built: list[str] = field(default_factory=list)
    promoted: int = 0


async def apply(plan: Plan, *, compiled: CompiledProject, engine: EngineAdapter, state: StateStore) -> ApplyResult:
    """Execute a plan against ``engine`` and record the result in ``state``."""
    result = ApplyResult()

    for task in plan.backfills:
        snapshot = task.snapshot
        model = compiled.models[snapshot.name]
        if model.ast is None:
            raise PlanError(f"executing Python model {snapshot.name!r} is not yet supported")

        resolved = resolve_model_query(model, compiled)
        relation = SqlRelation(
            ast=resolved, engine=EngineRef(name="default", dialect=model.dialect), schema=empty_schema()
        )
        strategy = resolve_strategy(model.materialise, model.strategy, model.key, model.time_column)

        await engine.create_schema(snapshot.physical_table.schema)
        statements = strategy.plan_statements(relation, snapshot.physical_table, engine.caps, task.interval)
        await engine.execute_all(statements)
        if model.columns:  # validate the built schema against the contract before recording it
            validate_contract(model.name, await engine.describe(snapshot.physical_table), model.columns)

        if task.interval is not None:  # incremental: accumulate the filled window in the ledger
            filled = (await state.get_intervals(snapshot.name, snapshot.fingerprint)).add(task.interval)
            snapshot = replace(snapshot, intervals=filled)
        await state.add_snapshot(snapshot)
        result.built.append(snapshot.name)

    for swap in plan.virtual_updates:
        await engine.create_schema(swap.view.schema)
        await engine.create_view(swap.view, swap.target)

    mapping = {name: model.fingerprint for name, model in compiled.models.items()}
    await state.promote(plan.environment, mapping)
    result.promoted = len(mapping)
    return result
