"""Compute a plan by diffing a compiled project against an environment.

For each model, compares the desired (compiled) fingerprint against the
fingerprint currently promoted in the target environment, classifying it as
ADDED / MODIFIED / REMOVED / UNCHANGED. A MODIFIED change is further classified
BREAKING vs NON_BREAKING, and each changed model gets an *impact* that drives
what actually gets rebuilt:

- ``semantic`` — the data of pre-existing columns may differ (changed
  expressions, filters, strategy config, Python source, or any semantic
  upstream). Rebuilt; downstream inherits BREAKING.
- ``additive`` — existing columns are provably identical, new columns appeared
  (strictly additive projections). Rebuilt; downstream inherits NON_BREAKING.
- ``clean`` — output is provably identical (only upstream fingerprints moved,
  and no additive upstream leaks in). **Not rebuilt**: the new snapshot reuses
  the previous physical table and the environment view repoints to it.

The clean case is the improvement over sqlmesh's model-granular invalidation.
It needs no column lineage: an indirectly-changed model's SQL is unchanged and
was previously valid, so it cannot reference newly-added upstream columns —
the only leak is a projection ``*`` (which inherits new columns), and Python
models, which see whole upstream tables and are treated as ``*``.
"""

from __future__ import annotations

from dataclasses import replace

from sqlglot import exp

from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.canonicalize import parse
from interlace.ir.fingerprint import canonical_sql
from interlace.plan.plan import ChangeType, ModelChange, Plan, ViewSwap, env_view, schedule_build
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
        engine=model.engine,
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


def _added_columns(previous_sql: str | None, ast: exp.Expression | None) -> tuple[str, ...] | None:
    """The strictly-added output columns of a direct change, or None if the change
    is not provably additive (i.e. pre-existing column data may differ)."""
    if previous_sql is None or not isinstance(ast, exp.Select):
        return None
    previous = parse(previous_sql)
    previous_map = _projection_map(previous)
    current_map = _projection_map(ast)
    if previous_map is None or current_map is None:
        return None
    added = [name for name in current_map if name not in previous_map]
    if not added or any(current_map.get(name) != expr for name, expr in previous_map.items()):
        return None  # nothing added, or an existing projection changed/was removed
    # strict: with the added projections removed, everything else (FROM, WHERE,
    # GROUP BY, ...) must be identical — a filter change is never additive
    stripped = ast.copy()
    stripped.set("expressions", [p for p in stripped.selects if p.alias_or_name not in set(added)])
    if canonical_sql(stripped) != canonical_sql(previous):
        return None
    return tuple(added)


def _selects_star(ast: exp.Expression | None) -> bool:
    """Whether the model's output can inherit new upstream columns."""
    if ast is None:
        return True  # Python models read whole upstream tables
    for select in ast.find_all(exp.Select):
        for projection in select.selects:
            if isinstance(projection, exp.Star):
                return True
            if isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star):
                return True  # qualified: t.*
    return False


def _schedule_reuse(plan: Plan, model: CompiledModel, previous: Snapshot, environment: str) -> None:
    """Record the new fingerprint over the previous physical table; build nothing."""
    if model.materialise == "ephemeral":
        return  # never physical; promotion alone carries it
    snapshot = replace(
        snapshot_of(model, ChangeCategory.NON_BREAKING),
        physical_table=previous.physical_table,
        intervals=previous.intervals,
    )
    plan.reuses.append(snapshot)
    if model.export is None and model.materialise in ("table", "view"):
        plan.virtual_updates.append(
            ViewSwap(env_view(environment, model.name), previous.physical_table, engine=model.engine)
        )


_HISTORY_STRATEGIES = frozenset({"merge_by_key", "full_merge", "scd_type_2", "scd2", "incremental_by_time"})
"""Strategies whose targets accumulate state a rebuild would destroy."""


async def diff(
    compiled: CompiledProject,
    environment: str,
    state: StateStore,
    *,
    select: set[str] | None = None,
    forward_only: bool = False,
) -> Plan:
    """Diff the compiled project against ``environment`` and return the plan.

    ``select`` limits which models are scheduled and promoted (None = all). Impact
    classification still runs over the whole graph so downstream categories are correct.

    ``forward_only``: modified models whose strategy accumulates history
    (merge_by_key / full_merge / scd_type_2 / incremental_by_time) inherit their
    previous physical table and interval ledger instead of starting fresh — the
    new logic applies going forward, history survives. Requires the new query to
    stay shape-compatible with the existing table.
    """
    selected = set(compiled.models) if select is None else select
    current = await state.get_environment(environment)
    plan = Plan(environment=environment)
    impact: dict[str, str] = {}  # changed models only: "semantic" | "additive" | "clean"

    for model in compiled.ordered():  # topo order: upstream impact known before downstream
        previous_fingerprint = current.get(model.name)

        if previous_fingerprint is None:
            impact[model.name] = "semantic"
            if model.name in selected:
                plan.changes.append(ModelChange(model.name, ChangeType.ADDED, None, None, model.fingerprint))
                schedule_build(plan, model, snapshot_of(model, ChangeCategory.BREAKING), environment)
            continue

        if previous_fingerprint == model.fingerprint:
            continue  # unchanged

        previous = await state.get_snapshot(model.name, previous_fingerprint)
        added: tuple[str, ...] = ()
        if previous is None or previous.local_fingerprint != model.local_fingerprint:
            # direct change: always rebuilds itself; additive-only narrows downstream impact
            columns = _added_columns(previous.definition_sql if previous else None, model.ast)
            semantic = columns is None
            added = columns or ()
            category = ChangeCategory.BREAKING if semantic else ChangeCategory.NON_BREAKING
            impact[model.name] = "semantic" if semantic else "additive"
            rebuild = True
        else:  # indirect: only upstream fingerprints moved
            upstream = [impact.get(dep, "clean") for dep in model.dependencies]
            if "semantic" in upstream:
                category, rebuild = ChangeCategory.BREAKING, True
                impact[model.name] = "semantic"
            elif "additive" in upstream and _selects_star(model.ast):
                category, rebuild = ChangeCategory.NON_BREAKING, True
                impact[model.name] = "additive"  # a * projection inherits the new columns
            else:
                category, rebuild = ChangeCategory.NON_BREAKING, False
                impact[model.name] = "clean"  # provably identical output

        if model.name not in selected:
            continue
        inherit = forward_only and rebuild and model.strategy in _HISTORY_STRATEGIES and previous is not None
        if inherit:
            category = ChangeCategory.FORWARD_ONLY
        plan.changes.append(
            ModelChange(model.name, ChangeType.MODIFIED, category, previous_fingerprint, model.fingerprint, added)
        )
        if inherit:  # keep history: new fingerprint over the previous physical table
            snapshot = replace(
                snapshot_of(model, ChangeCategory.FORWARD_ONLY),
                physical_table=previous.physical_table,  # type: ignore[union-attr]
                intervals=previous.intervals,  # type: ignore[union-attr]
            )
            schedule_build(plan, model, snapshot, environment)
        elif rebuild:
            schedule_build(plan, model, snapshot_of(model, category), environment)
        else:
            _schedule_reuse(plan, model, previous, environment)  # type: ignore[arg-type]  # previous is not None here

    if select is None:
        for removed in sorted(set(current) - set(compiled.models)):
            plan.changes.append(ModelChange(removed, ChangeType.REMOVED, None, current[removed], None))

    plan.promote = sorted(selected)
    return plan
