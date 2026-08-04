"""Column-level lineage.

For each model output column, which upstream ``(table, column)`` it derives from.
Built in topological order: each model's SQL is qualified against a schema graph
accumulated from upstream models, ``*`` and unqualified columns are resolved, and
the columns feeding each projection are extracted. Best-effort — a model whose
SQL can't be qualified (e.g. a Python model, or an unresolvable query) yields no
column lineage rather than failing the whole project.
"""

from __future__ import annotations

from typing import Any

from sqlglot import exp
from sqlglot.optimizer.qualify import qualify

from interlace.graph.project import CompiledProject

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


def _model_lineage(ast: exp.Expression, schema: dict[str, Any], dialect: str) -> ColumnSources | None:
    try:
        qualified = qualify(ast.copy(), schema=schema, dialect=dialect)
    except Exception:
        return None
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


def column_lineage(project: CompiledProject) -> dict[str, ColumnSources]:
    """Compute per-model column lineage for the whole project (topological order)."""
    schema: dict[str, Any] = {}
    result: dict[str, ColumnSources] = {}
    for name in project.graph.topological_sort():
        model = project.models[name]
        if model.ast is None:
            result[name] = {}
            continue
        lineage = _model_lineage(model.ast, schema, model.dialect)
        result[name] = lineage or {}
        if lineage is not None:
            _insert_schema(schema, name, dict.fromkeys(lineage, "UNKNOWN"))
    return result


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


def column_impact(project: CompiledProject, model: str, column: str) -> dict[str, Any]:
    """Column-level blast radius of ``model.column``: every downstream column
    transitively derived from it, plus *opaque* consumers (Python models or ``*``
    projections) that read the source model whole and so may touch every column.

    Returns ``{"source": "model.column", "impacted": [{model, column, via}, ...],
    "opaque_consumers": [model, ...]}``.
    """
    lineage_map = column_lineage(project)
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
    opaque = sorted(name for name, m in project.models.items() if model in m.dependencies and not lineage_map.get(name))
    return {"source": f"{model}.{column}", "impacted": impacted, "opaque_consumers": opaque}
