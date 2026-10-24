"""Column-level lineage.

For each model output column, which upstream ``(table, column)`` it derives from.
Built in topological order: each model's SQL is qualified against a schema graph
accumulated from upstream models, ``*`` and unqualified columns are resolved, and
the columns feeding each projection are extracted.

A model whose SQL can't be qualified — a Python model (no AST), or a query that
references a column we don't know an upstream has — is *opaque*: we can't trace
inside it. But it must still contribute its output columns to the schema graph,
or every SQL model downstream of it fails to qualify too (sqlglot's ``qualify``
raises when a query references a table missing from a non-empty schema). So an
opaque model's columns come from the best source available — a supplied
``known_columns`` hint (e.g. the warehouse-described columns), its declared
``columns`` contract, its own SELECT's projection names, or, failing all that,
the union of its upstreams' columns (name-passthrough) — and its lineage maps
each output column to any same-named upstream column. Best-effort, but it keeps
the trace alive through the opaque node instead of dead-ending the whole subtree.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from typing import Any

from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import MappingSchema

from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.canonicalize import is_star_projection

ColumnSources = dict[str, list[tuple[str, str]]]  # output column -> [(table, column), ...]


def _table_name(table: exp.Table) -> str:
    return f"{table.db}.{table.name}" if table.db else table.name


def _insert_schema(schema: dict[str, Any], model_name: str, columns: dict[str, str]) -> None:
    """Insert a model's columns into the (possibly nested) schema graph by name parts."""
    *namespaces, leaf = model_name.split(".")
    node = schema
    for namespace in namespaces:
        node = node.setdefault(namespace, {})
    node[leaf] = columns


def _model_lineage(ast: exp.Expr, schema: dict[str, Any], dialect: str) -> ColumnSources | None:
    qualified = _qualify(ast, schema, dialect)
    if not isinstance(qualified, exp.Select):
        return None

    aliases = {table.alias_or_name: _table_name(table) for table in qualified.find_all(exp.Table)}
    lineage: ColumnSources = {}
    for projection in qualified.selects:
        sources: list[tuple[str, str]] = []
        for column in projection.find_all(exp.Column):
            ref = (aliases.get(column.table, column.table), column.name)
            if ref not in sources:
                sources.append(ref)
        lineage[projection.alias_or_name] = sources
    return lineage


def _qualify(ast: exp.Expr, schema: dict[str, Any], dialect: str) -> exp.Expr | None:
    """Qualify ``ast``. A table that is not a model has no entry in ``schema``.

    The compiler used to attach an unresolved column to the single such table.
    It no longer does, so seed that table with the columns this query names
    and qualify again. A query that is still ambiguous stays opaque.
    """
    try:
        return qualify(ast.copy(), schema=schema, dialect=dialect)
    except Exception:
        seeded = _seed_unknown_tables(ast, schema, dialect)
        if seeded is None:
            return None
    try:
        return qualify(ast.copy(), schema=seeded, dialect=dialect)
    except Exception:
        return None


def _seed_unknown_tables(ast: exp.Expr, schema: dict[str, Any], dialect: str) -> MappingSchema | None:
    """Columns a query names on tables missing from ``schema``, keyed for qualify."""
    try:
        mapping = MappingSchema(schema, dialect=dialect)
        pending: dict[str, tuple[exp.Table, dict[str, str]]] = {}
        for scope in traverse_scope(ast):
            _collect_unknown_columns(scope, mapping, pending, dialect)
        if not any(columns for _, columns in pending.values()):
            return None
        seeded = MappingSchema(copy.deepcopy(schema), dialect=dialect)
        for target, columns in pending.values():
            seeded.add_table(target, columns)
    except Exception:
        return None
    return seeded


def _collect_unknown_columns(
    scope: Scope,
    mapping: MappingSchema,
    pending: dict[str, tuple[exp.Table, dict[str, str]]],
    dialect: str,
) -> None:
    unknown, known = _scope_columns(scope, mapping)
    for alias, table in unknown:
        target = _schema_table(table, mapping.depth())
        if target is None:
            continue
        bucket = pending.setdefault(target.sql(dialect=dialect), (target, {}))[1]
        for name in _named(scope.source_columns(alias)):
            bucket[name] = "UNKNOWN"
        if len(unknown) == 1:
            for name in _named(scope.unqualified_columns):
                if name not in known:
                    bucket[name] = "UNKNOWN"


def _scope_columns(scope: Scope, mapping: MappingSchema) -> tuple[list[tuple[str, exp.Table]], set[str]]:
    """Sources with no schema columns, and column names the known sources already have."""
    unknown: list[tuple[str, exp.Table]] = []
    known: set[str] = set()
    for alias, (_, source) in scope.selected_sources.items():
        if isinstance(source, exp.Table):
            columns = mapping.column_names(source)
            if columns:
                known.update(columns)
            else:
                unknown.append((alias, source))
        elif isinstance(source, Scope) and isinstance(source.expression, exp.Selectable):
            known.update(source.expression.named_selects)
    return unknown, known


def _named(columns: list[exp.Column]) -> list[str]:
    return [column.name for column in columns if column.name and column.name != "*"]


def _schema_table(table: exp.Table, depth: int) -> exp.Table | None:
    """The schema key for ``table``. An empty schema has depth 0 and takes the bare name."""
    if depth <= 1:
        return exp.to_table(table.name) if table.name else None
    if len(table.parts) != depth:
        return None
    return table.copy()


def _projection_names(model: CompiledModel) -> list[str]:
    """Output column names read straight off a SELECT, or [] if it projects a star
    (which we can't name without expanding it against the upstream schema)."""
    if not isinstance(model.ast, exp.Select):
        return []
    names: list[str] = []
    for projection in model.ast.selects:
        if is_star_projection(projection):
            return []
        names.append(projection.alias_or_name)
    return names


def _opaque_columns(
    model: CompiledModel, known_columns: Mapping[str, Sequence[str]], columns_by_model: Mapping[str, list[str]]
) -> list[str]:
    """Best-effort output columns for a model we couldn't qualify, in priority order:
    a supplied hint (warehouse-described), the declared contract, this model's own
    projection names, then the union of its upstreams' columns (name-passthrough)."""
    hint = known_columns.get(model.name)
    if hint:
        return list(hint)
    if model.columns:
        return list(model.columns)
    projected = _projection_names(model)
    if projected:
        return projected
    passthrough: list[str] = []
    seen: set[str] = set()
    for dep in model.dependencies:
        for column in columns_by_model.get(dep, ()):
            if column not in seen:
                seen.add(column)
                passthrough.append(column)
    return passthrough


def _passthrough_lineage(
    columns: list[str], dependencies: Sequence[str], columns_by_model: Mapping[str, list[str]]
) -> ColumnSources:
    """Attribute each output column to any direct-upstream column of the same name.
    A column with no name match (one the opaque model introduces) originates here."""
    return {
        column: [(dep, column) for dep in dependencies if column in columns_by_model.get(dep, ())] for column in columns
    }


def _compute(project: CompiledProject, hint: Mapping[str, Sequence[str]]) -> tuple[dict[str, ColumnSources], set[str]]:
    """The shared pass: per-model column lineage plus the set of *opaque* models —
    those we couldn't qualify (Python models, or SQL over an unknown column), whose
    lineage is name-passthrough rather than a precise trace."""
    schema: dict[str, Any] = {}
    result: dict[str, ColumnSources] = {}
    columns_by_model: dict[str, list[str]] = {}
    opaque: set[str] = set()
    for name in project.graph.topological_sort():
        model = project.models[name]
        lineage = _model_lineage(model.ast, schema, model.dialect) if model.ast is not None else None
        if lineage is not None:
            columns = list(lineage)
        else:  # opaque: a Python model, or SQL that referenced an unknown column
            opaque.add(name)
            columns = _opaque_columns(model, hint, columns_by_model)
            lineage = _passthrough_lineage(columns, model.dependencies, columns_by_model)
        result[name] = lineage
        columns_by_model[name] = columns
        if columns:  # seed the schema graph so models downstream of this one qualify
            _insert_schema(schema, name, dict.fromkeys(columns, "UNKNOWN"))
    return result, opaque


def column_lineage(
    project: CompiledProject, known_columns: Mapping[str, Sequence[str]] | None = None
) -> dict[str, ColumnSources]:
    """Compute per-model column lineage for the whole project (topological order).

    ``known_columns`` optionally supplies real output columns per model (e.g. from
    the warehouse); it lets a model whose SQL can't be qualified — a Python model,
    above all — still seed the schema graph so downstream models qualify, and lets
    ``SELECT *`` expand. Without it, an opaque model falls back to its declared
    contract, its projection names, or its upstreams' columns.
    """
    return _compute(project, known_columns or {})[0]


def split_target(target: str, project: CompiledProject) -> tuple[str, str] | None:
    """Parse a ``model.column`` string into ``(model, column)``, tolerating dotted
    model names (``schema.model.column``). Returns None if no known model matches."""
    dot = target.rfind(".")
    if dot <= 0:
        return None
    model, column = target[:dot], target[dot + 1 :]
    while model and model not in project.models and "." in model:
        dot = model.rfind(".")
        model, column = model[:dot], f"{model[dot + 1 :]}.{column}"
    return (model, column) if model in project.models and column else None


def column_impact(
    project: CompiledProject, model: str, column: str, known_columns: Mapping[str, Sequence[str]] | None = None
) -> dict[str, Any]:
    """Column-level blast radius of ``model.column``: every downstream column
    transitively derived from it, plus *opaque* consumers (Python models or ``*``
    projections) that read the source model whole and so may touch every column.

    Returns ``{"source": "model.column", "impacted": [{model, column, via}, ...],
    "opaque_consumers": [model, ...]}``.
    """
    lineage_map, opaque_models = _compute(project, known_columns or {})
    column_down: dict[str, list[tuple[str, str]]] = {}
    for downstream, sources in lineage_map.items():
        for down_col, refs in sources.items():
            for up_model, up_col in refs:
                column_down.setdefault(f"{up_model}.{up_col}", []).append((downstream, down_col))

    impacted: list[dict[str, str]] = []
    seen = {f"{model}.{column}"}
    frontier = [f"{model}.{column}"]
    while frontier:
        key = frontier.pop()
        for down_model, down_col in column_down.get(key, ()):
            next_key = f"{down_model}.{down_col}"
            if next_key not in seen:
                seen.add(next_key)
                impacted.append({"model": down_model, "column": down_col, "via": key})
                frontier.append(next_key)
    # opaque direct consumers: a Python model (or unqualifiable SQL) derives columns
    # from the source in ways name-passthrough can't see, so flag it — check it whole.
    opaque = sorted(name for name in opaque_models if model in project.models[name].dependencies)
    return {"source": f"{model}.{column}", "impacted": impacted, "opaque_consumers": opaque}
