"""Parsing and static analysis of SQL into the canonical IR.

Dialect-aware parsing plus table-reference extraction for implicit dependency
discovery and reference rewriting. Column-level qualification against the project
schema graph lives in ``graph/column_lineage`` (used by the differ's column
pruning and the ``impact`` command).
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from interlace.exceptions import CompilationError
from interlace.ir.relation import TableRef


def parse(sql: str, dialect: str = "duckdb") -> exp.Expression:
    """Parse a single SQL statement in the given dialect into a sqlglot AST."""
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except Exception as exc:  # sqlglot raises ParseError and friends
        # sqlglot's str() is a multi-line, ANSI-highlighted snippet; the first line
        # ("Expecting ). Line 7, Col: 56.") is the actionable summary. Keep the full
        # text in details for the API/event log; surface the summary in the message.
        summary = next((line for line in str(exc).splitlines() if line.strip()), exc.__class__.__name__)
        raise CompilationError(
            f"failed to parse SQL ({dialect}): {summary}", details={"sql": sql, "error": str(exc)}
        ) from exc
    if parsed is None:
        raise CompilationError("empty SQL statement", details={"sql": sql})
    return parsed


def table_references(ast: exp.Expression) -> list[str]:
    """Return the distinct real table references in an AST, excluding CTE names.

    Each reference is keyed as ``db.name`` (or ``name`` when unqualified). CTE
    aliases are filtered out — they are local names, not dependencies.
    """
    cte_names = {cte.alias_or_name for cte in ast.find_all(exp.CTE)}
    refs: list[str] = []
    seen: set[str] = set()
    for table in ast.find_all(exp.Table):
        name = table.name
        if not name or name in cte_names:
            continue
        key = f"{table.db}.{name}" if table.db else name
        if key not in seen:
            seen.add(key)
            refs.append(key)
    return refs


def resolve_references(ast: exp.Expression, mapping: dict[str, TableRef]) -> exp.Expression:
    """Rewrite model-name table references to their physical tables (returns a new AST).

    ``mapping`` is keyed by dependency model name; a reference matches by its
    ``db.name`` key or, failing that, its bare name (so ``main.orders`` resolves
    to model ``orders``). Aliases are preserved; the input AST is not mutated.
    """
    if not mapping:
        return ast

    def rewrite(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Table):
            key = f"{node.db}.{node.name}" if node.db else node.name
            target = mapping.get(key) or mapping.get(node.name)
            if target is not None:
                if not node.alias:  # keep qualified column refs (b.x) resolving after the rename
                    node.set("alias", exp.TableAlias(this=exp.to_identifier(node.name)))
                node.set("this", exp.to_identifier(target.name))
                node.set("db", exp.to_identifier(target.schema) if target.schema else None)
                node.set("catalog", exp.to_identifier(target.catalog) if target.catalog else None)
        return node

    return ast.transform(rewrite)
