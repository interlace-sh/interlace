"""Compute a plan by diffing a compiled project against an environment.

For each model, compares the desired (compiled) fingerprint against the
fingerprint currently promoted in the target environment, classifying it as
ADDED / MODIFIED / REMOVED / UNCHANGED. A MODIFIED change is further classified
BREAKING vs NON_BREAKING:

- **direct** change (the model's own SQL/config changed — detected by a changed
  *local* fingerprint): non-breaking iff the change is purely additive output
  columns, else breaking;
- **indirect** change (local fingerprint unchanged, only an upstream moved):
  inherits BREAKING if any changed upstream is breaking, else NON_BREAKING.

v1 builds every changed model (ADDED ∪ MODIFIED). The classification is computed
and surfaced now; using it to *skip* rebuilds via physical-table reuse
("indirect non-breaking") lands with the column-lineage phase (§7).
"""

from __future__ import annotations

from sqlglot import exp

from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.canonicalize import parse
from interlace.plan.plan import ChangeType, ModelChange, Plan, schedule_build
from interlace.state.snapshot import ChangeCategory, Snapshot
from interlace.state.store import StateStore


def snapshot_of(model: CompiledModel, category: ChangeCategory) -> Snapshot:
    """Build the persistable Snapshot for a compiled model's new version."""
    return Snapshot(
        name=model.name,
        fingerprint=model.fingerprint,
        metadata_hash=model.metadata_hash,
        physical_table=model.physical_table,
        change_category=category,
        local_fingerprint=model.local_fingerprint,
        definition_sql=model.definition_sql,
    )


def _projection_map(ast: exp.Expression | None) -> dict[str, str] | None:
    """Map output column name -> expression SQL for a simple SELECT, or None if undeterminable."""
    if not isinstance(ast, exp.Select):
        return None
    columns: dict[str, str] = {}
    for projection in ast.selects:
        if projection.find(exp.Star) is not None:
            return None  # star -> output columns unknown without a schema
        expr = projection.this if isinstance(projection, exp.Alias) else projection
        columns[projection.alias_or_name] = expr.sql()
    return columns


def _classify_direct(previous_sql: str | None, new_ast: exp.Expression | None) -> ChangeCategory:
    """A direct change is NON_BREAKING iff it only *adds* output columns; else BREAKING."""
    if previous_sql is None or new_ast is None:
        return ChangeCategory.BREAKING
    previous = _projection_map(parse(previous_sql))
    current = _projection_map(new_ast)
    if previous is None or current is None:
        return ChangeCategory.BREAKING
    if all(current.get(name) == expr for name, expr in previous.items()):
        return ChangeCategory.NON_BREAKING
    return ChangeCategory.BREAKING


async def diff(
    compiled: CompiledProject, environment: str, state: StateStore, *, select: set[str] | None = None
) -> Plan:
    """Diff the compiled project against ``environment`` and return the plan.

    ``select`` limits which models are scheduled and promoted (None = all). Change
    classification still runs over the whole graph so downstream categories are correct.
    """
    selected = set(compiled.models) if select is None else select
    current = await state.get_environment(environment)
    plan = Plan(environment=environment)
    categories: dict[str, ChangeCategory] = {}

    for model in compiled.ordered():  # topo order: upstream categories known before downstream
        previous_fingerprint = current.get(model.name)

        if previous_fingerprint is None:
            categories[model.name] = ChangeCategory.BREAKING
            if model.name in selected:
                plan.changes.append(ModelChange(model.name, ChangeType.ADDED, None, None, model.fingerprint))
                schedule_build(plan, model, snapshot_of(model, ChangeCategory.BREAKING), environment)
            continue

        if previous_fingerprint == model.fingerprint:
            continue  # unchanged

        previous = await state.get_snapshot(model.name, previous_fingerprint)
        if previous is None or previous.local_fingerprint != model.local_fingerprint:
            category = _classify_direct(previous.definition_sql if previous else None, model.ast)
        else:
            upstream_breaking = any(categories.get(dep) is ChangeCategory.BREAKING for dep in model.dependencies)
            category = ChangeCategory.BREAKING if upstream_breaking else ChangeCategory.NON_BREAKING

        categories[model.name] = category
        if model.name in selected:
            plan.changes.append(
                ModelChange(model.name, ChangeType.MODIFIED, category, previous_fingerprint, model.fingerprint)
            )
            schedule_build(plan, model, snapshot_of(model, category), environment)

    if select is None:
        for removed in sorted(set(current) - set(compiled.models)):
            plan.changes.append(ModelChange(removed, ChangeType.REMOVED, None, current[removed], None))

    plan.promote = sorted(selected)
    return plan
