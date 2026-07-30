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

Column pruning extends the clean case to *semantic* upstream changes: when a
direct change provably touched only specific output columns (projection-only
edit — FROM/WHERE/GROUP BY identical, so the row set is untouched), a
downstream that provably consumes none of the touched columns is still clean.
Both proofs are conservative: any ambiguity (``*``, unattributable unqualified
references, DISTINCT, aliases leaking into other clauses) falls back to
"everything touched" / "everything consumed" and the model rebuilds.
"""

from __future__ import annotations

from dataclasses import replace

from sqlglot import exp

from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.canonicalize import parse
from interlace.ir.fingerprint import canonical_sql
from interlace.plan.plan import ChangeType, ModelChange, Plan, ViewSwap, collect_transfers, env_view, schedule_build
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
        if projection.alias_or_name in columns:
            return None  # duplicate output name: positional comparison is unsound
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


def _changed_columns(previous_sql: str | None, ast: exp.Expression | None) -> frozenset[str] | None:
    """The output columns whose data may differ after a direct change, or None
    when the touched set cannot be proven (assume every column changed).

    Provable only for a projection-level edit: with both projection lists
    erased the queries must be canonically identical, so the row set (FROM,
    WHERE, GROUP BY, ...) is untouched and unchanged projections stay
    byte-identical. Anything that lets a changed alias leak beyond its own
    projection — DISTINCT, non-column grouping (positions, expressions),
    references to a changed name from other clauses or sibling projections
    (lateral aliases) — falls back to None.
    """
    if previous_sql is None or not isinstance(ast, exp.Select):
        return None
    previous = parse(previous_sql)
    previous_map = _projection_map(previous)
    current_map = _projection_map(ast)
    if previous_map is None or current_map is None:
        return None
    stripped_previous = previous.copy()
    stripped_previous.set("expressions", [exp.Star()])
    stripped_current = ast.copy()
    stripped_current.set("expressions", [exp.Star()])
    if canonical_sql(stripped_current) != canonical_sql(stripped_previous):
        return None  # row-set change: every column is touched
    changed = {name for name, expr in previous_map.items() if current_map.get(name) != expr}
    if not changed:
        return frozenset()
    if ast.args.get("distinct"):
        return None  # DISTINCT dedups over all columns: any change reshapes the row set
    group = ast.args.get("group")
    if group is not None and not all(isinstance(e, exp.Column) for e in group.expressions):
        return None  # positional / computed grouping can silently track a changed projection
    leaky = [c for c in stripped_current.find_all(exp.Column) if not c.table and c.name in changed]
    for projection in ast.selects:
        if projection.alias_or_name in changed:
            continue
        leaky.extend(c for c in projection.find_all(exp.Column) if not c.table and c.name in changed)
    return None if leaky else frozenset(changed)


def _consumed_columns(ast: exp.Expression | None, dependency: str) -> frozenset[str] | None:
    """The columns of ``dependency`` that ``ast`` provably reads, or None when
    attribution is impossible (Python models, ``*`` projections, unqualified
    references in a multi-source query) — None means "assume every column".

    Matching mirrors :func:`resolve_references`: a table names the dependency
    by its ``db.name`` key or its bare name. Ambiguity errs toward *consuming
    more* (a shadowed alias attributes to the dependency; a CTE reference
    counts as an extra source), never less.
    """
    if ast is None or _selects_star(ast):
        return None
    cte_names = {cte.alias_or_name for cte in ast.find_all(exp.CTE)}
    aliases: set[str] = set()
    sources: set[str] = set()
    for table in ast.find_all(exp.Table):
        if not table.name:
            continue
        key = f"{table.db}.{table.name}" if table.db else table.name
        sources.add(key)
        if table.name not in cte_names and dependency in (key, table.name):
            aliases.add(table.alias_or_name)
    consumed: set[str] = set()
    for column in ast.find_all(exp.Column):
        if not column.table:
            if len(sources) > 1:
                return None  # unqualified in a multi-source query: not attributable
            consumed.add(column.name)
        elif column.table in aliases:
            consumed.add(column.name)
    return frozenset(consumed)


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
    touched: dict[str, frozenset[str] | None] = {}  # semantic models: provably-changed columns (None = all)

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
            previous_sql = previous.definition_sql if previous else None
            columns = _added_columns(previous_sql, model.ast)
            semantic = columns is None
            added = columns or ()
            category = ChangeCategory.BREAKING if semantic else ChangeCategory.NON_BREAKING
            impact[model.name] = "semantic" if semantic else "additive"
            if semantic:
                touched[model.name] = _changed_columns(previous_sql, model.ast)
            rebuild = True
        else:  # indirect: only upstream fingerprints moved
            verdict = "clean"
            for dep in model.dependencies:
                dep_impact = impact.get(dep, "clean")
                if dep_impact == "semantic":
                    dep_touched = touched.get(dep)  # None (or unset) = every column may differ
                    consumed = _consumed_columns(model.ast, dep)
                    if dep_touched is not None and consumed is not None and not (consumed & dep_touched):
                        continue  # column-pruned: reads only provably-unchanged columns
                    verdict = "semantic"
                    break
                if dep_impact == "additive" and _selects_star(model.ast):
                    verdict = "additive"  # a * projection inherits the new columns
            impact[model.name] = verdict
            if verdict == "semantic":
                category, rebuild = ChangeCategory.BREAKING, True
            elif verdict == "additive":
                category, rebuild = ChangeCategory.NON_BREAKING, True
            else:
                category, rebuild = ChangeCategory.NON_BREAKING, False  # provably identical output

        if model.name not in selected:
            continue
        inherit = (
            forward_only
            and rebuild
            and model.strategy in _HISTORY_STRATEGIES
            and previous is not None
            and previous.engine == model.engine  # history can't be copied across engines
        )
        if inherit:
            category = ChangeCategory.FORWARD_ONLY
        plan.changes.append(
            ModelChange(model.name, ChangeType.MODIFIED, category, previous_fingerprint, model.fingerprint, added)
        )
        if inherit:  # copy-on-write: history seeds the NEW table; checks gate before views move
            snapshot = replace(
                snapshot_of(model, ChangeCategory.FORWARD_ONLY),
                intervals=previous.intervals,  # type: ignore[union-attr]
            )
            schedule_build(plan, model, snapshot, environment, seed_from=previous.physical_table)  # type: ignore[union-attr]
        elif rebuild:
            schedule_build(plan, model, snapshot_of(model, category), environment)
        else:
            _schedule_reuse(plan, model, previous, environment)  # type: ignore[arg-type]  # previous is not None here

    if select is None:
        for removed in sorted(set(current) - set(compiled.models)):
            plan.changes.append(ModelChange(removed, ChangeType.REMOVED, None, current[removed], None))

    plan.promote = sorted(selected)
    plan.transfers = collect_transfers(compiled, [task.snapshot.name for task in plan.backfills])
    return plan
