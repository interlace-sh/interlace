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

One model is built in :mod:`interlace.plan.backfill`. Column fitting, external
delivery, and cross-engine staging live beside it. :mod:`interlace.plan.schedule`
orders the builds. This module promotes the environment.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from interlace.engines.base import EngineAdapter
from interlace.engines.registry import EngineRegistry, as_registry
from interlace.exceptions import PlanError
from interlace.graph.project import CompiledProject
from interlace.ir.relation import TableRef, drop
from interlace.plan.annotate import annotate_plan
from interlace.plan.delivery import _physical_ddl
from interlace.plan.fit import _remember
from interlace.plan.plan import ChangeType, Plan, env_view
from interlace.plan.result import ApplyResult, ProgressCallback
from interlace.plan.schedule import schedule_builds
from interlace.sinks import target_ref
from interlace.state.store import StateStore


async def apply(  # noqa: C901
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
        adapter.refresh_inputs()
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

    await schedule_builds(
        plan,
        compiled,
        registry,
        physical,
        state,
        base_path,
        result,
        parallelism,
        on_progress,
        connections,
    )

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
