"""Parsing and static analysis of SQL into the canonical IR.

Phase 1 covers parsing (dialect-aware) and table-reference extraction for
implicit dependency discovery. Qualification and type annotation
(``sqlglot.optimizer.qualify`` against the project schema graph) land alongside
column lineage in a later phase.
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
        raise CompilationError(f"failed to parse SQL ({dialect})", details={"sql": sql, "error": str(exc)}) from exc
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
                node.set("this", exp.to_identifier(target.name))
                node.set("db", exp.to_identifier(target.schema) if target.schema else None)
                node.set("catalog", exp.to_identifier(target.catalog) if target.catalog else None)
        return node

    return ast.transform(rewrite)
