"""Fill a plan's physical lines and external-table drift from live engines.

``diff`` can see the declared spec and the objects recorded in state. It cannot
see engine caps or the live catalog. This pass runs once before ``plan`` is
shown or applied.
"""

from __future__ import annotations

from interlace.engines.registry import EngineRegistry
from interlace.graph.project import CompiledProject
from interlace.physical.drift import column_drift
from interlace.physical.reconcile import model_objects, object_changes
from interlace.plan.plan import DriftNote, PhysicalAction, PhysicalChange, Plan
from interlace.sinks import target_ref


async def annotate_plan(plan: Plan, compiled: CompiledProject, registry: EngineRegistry) -> None:
    """Rewrite physical actions with the engine's real caps, and report external drift."""
    if plan.annotated:
        return
    plan.annotated = True
    rewritten: list[PhysicalAction] = []
    for action in plan.physical:
        model = compiled.models[action.name]
        caps = registry.require(model.engine, model=model.name).caps
        desired, warnings = model_objects(model, caps)
        changes, _drops = object_changes(desired, action.previous)
        rewritten.append(
            PhysicalAction(
                name=action.name,
                standalone=action.standalone,
                previous=action.previous,
                changes=tuple(PhysicalChange(op, kind, name) for op, kind, name in changes),
                warnings=tuple(dict.fromkeys((*action.warnings, *warnings))),
            )
        )
        for warning in warnings:
            if warning not in plan.warnings:
                plan.warnings.append(warning)
        if not model.is_terminal:
            table = model.physical_table
            engine = registry.require(model.engine, model=model.name)
            if await engine.table_exists(table):
                desired_names = {obj.name for obj in desired}
                for index in await engine.list_indexes(table):
                    if index not in desired_names:
                        plan.drift.append(
                            DriftNote(
                                action.name,
                                f"{action.name}: index {index} on {table.schema}.{table.name} "
                                "is not managed by this model; left in place",
                            )
                        )
    plan.physical = rewritten

    for name in plan.promote:
        terminal = compiled.models.get(name)
        if terminal is None or terminal.materialise != "table" or not terminal.target:
            continue
        engine = registry.require(terminal.engine, model=terminal.name)
        target = target_ref(terminal.target)
        if not await engine.table_exists(target):
            continue
        desired, _warnings = model_objects(terminal, engine.caps)
        desired_names = {obj.name for obj in desired}
        for index in await engine.list_indexes(target):
            if index not in desired_names:
                plan.drift.append(
                    DriftNote(
                        name,
                        f"{name}: index {index} on {terminal.target} is not managed by this model; left in place",
                    )
                )
        if terminal.columns:
            live = await engine.describe(target)
            plan.drift.extend(
                column_drift(name, terminal.target, terminal.schema_policy.columns, live, terminal.columns)
            )
    plan.blocking.extend(note.message for note in plan.drift if note.blocking)
