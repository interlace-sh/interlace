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
from interlace.ir.canonicalize import is_star_projection, parse
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


def _fold(name: str) -> str:
    """Case-fold an identifier for comparison. ``definition_sql`` is stored via
    ``canonical_sql`` (normalize=True → unquoted identifiers lowercased) while the
    live AST keeps author case; folding both sides keeps the touched/consumed sets
    comparable. Over-merging distinct quoted-case names only enlarges an
    intersection — a rebuild, never a false skip."""
    return name.lower()


def _projection_map(ast: exp.Expression | None) -> dict[str, str] | None:
    """Map folded output column name -> canonical expression SQL for a simple
    SELECT, or None if undeterminable."""
    if not isinstance(ast, exp.Select):
        return None
    columns: dict[str, str] = {}
    for projection in ast.selects:
        if is_star_projection(projection):
            return None  # bare * / t.* / COLUMNS(regex): output columns unknown without a schema
        expr = projection.this if isinstance(projection, exp.Alias) else projection
        name = _fold(projection.alias_or_name)
        if name in columns:
            return None  # duplicate output name: positional comparison is unsound
        columns[name] = canonical_sql(expr)
    return columns


def _positional_hazard(ast: exp.Select) -> bool:
    """Whether a clause resolves against projection *positions* or the whole
    projection list, so a projection edit can silently leak into the row set:
    ``GROUP BY ALL`` / empty grouping, ordinals in GROUP BY / ORDER BY."""
    group = ast.args.get("group")
    if group is not None and (group.args.get("all") or not group.expressions):
        return True
    ordinals = list(group.expressions) if group is not None else []
    order = ast.args.get("order")
    if order is not None:
        ordinals.extend(item.this for item in order.expressions)
    return any(isinstance(e, exp.Literal) for e in ordinals)


def _direct_impact(
    previous_sql: str | None, ast: exp.Expression | None
) -> tuple[tuple[str, ...] | None, frozenset[str] | None]:
    """Prove what a direct change did to the model's output: ``(added, touched)``.

    ``added`` — the strictly-added output columns, or None if the change is not
    provably additive (pre-existing column data may differ). ``touched`` — the
    output columns whose data may differ, or None when that set cannot be proven
    (assume every column changed).

    Both proofs hold only for projection-level edits: with the projection lists
    erased (or the added projections removed) the queries must be canonically
    identical, so the row set (FROM, WHERE, GROUP BY, ...) is untouched. Anything
    that lets a projection edit leak beyond its own column — DISTINCT (dedups
    over all columns), GROUP BY ALL / ordinals (track the projection list
    positionally), a changed alias referenced from other clauses or sibling
    projections (lateral aliases) — falls back to the conservative answer.
    """
    if previous_sql is None or not isinstance(ast, exp.Select):
        return None, None
    try:
        previous = parse(previous_sql)
    except Exception:
        return None, None  # non-roundtripping construct: conservative, never a crash
    previous_map = _projection_map(previous)
    current_map = _projection_map(ast)
    if previous_map is None or current_map is None or not isinstance(previous, exp.Select):
        return None, None
    if ast.args.get("distinct") or _positional_hazard(ast) or _positional_hazard(previous):
        return None, None

    added = tuple(name for name in current_map if name not in previous_map)
    changed = {name for name, expr in previous_map.items() if current_map.get(name) != expr}
    if not added and not changed:
        # identical projections and (below would confirm) identical shape: the change
        # is outside the SQL — strategy/materialise config — which these proofs
        # cannot reason about. Everything may differ.
        return None, None

    if added and not changed:
        # additive candidate: with the added projections removed, everything else
        # (FROM, WHERE, GROUP BY, ...) must be identical — a filter change is never additive
        stripped = ast.copy()
        stripped.set("expressions", [p for p in stripped.selects if _fold(p.alias_or_name) not in set(added)])
        if canonical_sql(stripped) == canonical_sql(previous):
            return added, frozenset()

    # touched proof: erase both projection lists; the remainder must be identical
    stripped_previous = previous.copy()
    stripped_previous.set("expressions", [exp.Star()])
    stripped_current = ast.copy()
    stripped_current.set("expressions", [exp.Star()])
    if canonical_sql(stripped_current) != canonical_sql(stripped_previous):
        return None, None  # row-set change: every column is touched
    if not changed:
        return None, frozenset()
    leaky = [c for c in stripped_current.find_all(exp.Column) if not c.table and _fold(c.name) in changed]
    for projection in ast.selects:
        if _fold(projection.alias_or_name) in changed:
            continue
        leaky.extend(c for c in projection.find_all(exp.Column) if not c.table and _fold(c.name) in changed)
    return None, (None if leaky else frozenset(changed))


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
            if projection.find(exp.Columns) is not None:
                return True  # DuckDB COLUMNS(regex): matches unknown future columns
    return False


def _consumed_columns(ast: exp.Expression | None, dependency: str) -> frozenset[str] | None:
    """The folded columns of ``dependency`` that ``ast`` provably reads, or None
    when attribution is impossible (Python models, ``*`` / COLUMNS projections,
    NATURAL joins, unqualified references in a multi-source query) — None means
    "assume every column".

    Matching mirrors :func:`resolve_references`: a table names the dependency
    by its ``db.name`` key or its bare name. Ambiguity errs toward *consuming
    more* (a shadowed alias attributes to the dependency; a CTE reference
    counts as an extra source; USING keys attribute to the dependency; a
    struct-field qualifier attributes its root column), never less.
    """
    if ast is None or _selects_star(ast):
        return None
    cte_names = {cte.alias_or_name for cte in ast.find_all(exp.CTE)}
    dep_aliases: set[str] = set()
    all_aliases: set[str] = set()
    sources: set[str] = set()
    for table in ast.find_all(exp.Table):
        if not table.name:
            continue
        key = f"{table.db}.{table.name}" if table.db else table.name
        sources.add(key)
        all_aliases.add(_fold(table.alias_or_name))
        if table.name not in cte_names and dependency in (key, table.name):
            dep_aliases.add(_fold(table.alias_or_name))
    consumed: set[str] = set()
    for join in ast.find_all(exp.Join):
        if join.method and join.method.upper() == "NATURAL":
            return None  # the key set is every shared column — unknowable statically
        for using in join.args.get("using") or []:
            consumed.add(_fold(using.name))  # USING keys read from both sides
    for column in ast.find_all(exp.Column):
        parts = column.parts
        if len(parts) == 1:
            if len(sources) > 1:
                return None  # unqualified in a multi-source query: not attributable
            consumed.add(_fold(column.name))
            continue
        head = _fold(parts[0].name)
        if head in dep_aliases:
            consumed.add(_fold(parts[1].name))  # alias.col / alias.struct.field -> col/struct
        elif head not in all_aliases:
            # not a table alias: struct/JSON access rooted at an unqualified column
            if len(sources) > 1:
                return None
            consumed.add(head)
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
    if model.materialise in ("virtual", "view"):  # terminal table/file has no env view to repoint
        plan.virtual_updates.append(
            ViewSwap(env_view(environment, model.name), previous.physical_table, engine=model.engine)
        )


_HISTORY_STRATEGIES = frozenset({"merge", "full_merge", "hash_merge", "scd", "incremental"})
"""Strategies whose targets accumulate state a rebuild would destroy."""


def _expand_to_changed_ancestors(compiled: CompiledProject, selected: set[str], current: dict[str, str]) -> set[str]:
    """Grow a selection to include every changed ancestor of a selected model.

    A selected downstream's new fingerprint hashes its upstreams' new
    fingerprints; building it while a changed upstream stays unscheduled would
    resolve against a physical table that was never built. Fingerprints are
    transitive, so an unchanged dependency proves its whole ancestry unchanged
    and the walk stops there.
    """
    expanded = set(selected)
    stack = [name for name in selected if name in compiled.models]
    while stack:
        for dep in compiled.models[stack.pop()].dependencies:
            upstream = compiled.models.get(dep)
            if upstream is None or dep in expanded:
                continue
            if current.get(dep) != upstream.fingerprint:
                expanded.add(dep)
                stack.append(dep)
    return expanded


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
    (merge / full_merge / scd / incremental) inherit their
    previous physical table and interval ledger instead of starting fresh — the
    new logic applies going forward, history survives. Requires the new query to
    stay shape-compatible with the existing table.
    """
    selected = set(compiled.models) if select is None else select
    current = await state.get_environment(environment)
    if select is not None:
        # a selected model must never build against an upstream fingerprint that was
        # never materialised: pull every changed ancestor of the selection in too
        selected = _expand_to_changed_ancestors(compiled, selected, current)
    plan = Plan(environment=environment)
    impact: dict[str, str] = {}  # changed models only: "semantic" | "additive" | "clean"
    touched: dict[str, frozenset[str] | None] = {}  # semantic models: provably-changed columns (None = all)

    previous_snapshots = await state.get_snapshots(
        (name, fingerprint)
        for name, fingerprint in current.items()
        if name in compiled.models and fingerprint != compiled.models[name].fingerprint
    )
    # Fingerprints already materialised (by any prior apply — most usefully another
    # environment): building these again would recompute an identical, content-addressed
    # table, so schedule a reuse (record + view-swap) instead of a rebuild.
    already_built = await state.get_snapshots((name, compiled.models[name].fingerprint) for name in selected)

    def is_materialised(model: CompiledModel) -> bool:
        return (model.name, model.fingerprint) in already_built

    for model in compiled.ordered():  # topo order: upstream impact known before downstream
        previous_fingerprint = current.get(model.name)

        if previous_fingerprint is None:
            impact[model.name] = "semantic"
            if model.name in selected:
                plan.changes.append(ModelChange(model.name, ChangeType.ADDED, None, None, model.fingerprint))
                schedule_build(
                    plan,
                    model,
                    snapshot_of(model, ChangeCategory.BREAKING),
                    environment,
                    reuse_existing=is_materialised(model),
                )
            continue

        if previous_fingerprint == model.fingerprint:
            continue  # unchanged

        previous = previous_snapshots.get((model.name, previous_fingerprint))
        added: tuple[str, ...] = ()
        if previous is None or previous.local_fingerprint != model.local_fingerprint:
            # direct change: always rebuilds itself; additive-only narrows downstream impact
            previous_sql = previous.definition_sql if previous else None
            columns, changed_columns = _direct_impact(previous_sql, model.ast)
            semantic = columns is None
            added = columns or ()
            category = ChangeCategory.BREAKING if semantic else ChangeCategory.NON_BREAKING
            impact[model.name] = "semantic" if semantic else "additive"
            if semantic:
                touched[model.name] = changed_columns
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
            and not model.is_terminal  # a terminal table is never dropped: inherently forward-only, nothing to seed
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
            schedule_build(
                plan, model, snapshot_of(model, category), environment, reuse_existing=is_materialised(model)
            )
        else:
            _schedule_reuse(plan, model, previous, environment)  # type: ignore[arg-type]  # previous is not None here

    if select is None:
        for removed in sorted(set(current) - set(compiled.models)):
            plan.changes.append(ModelChange(removed, ChangeType.REMOVED, None, current[removed], None))

    plan.promote = sorted(selected)
    plan.transfers = collect_transfers(compiled, [task.snapshot.name for task in plan.backfills])
    return plan
