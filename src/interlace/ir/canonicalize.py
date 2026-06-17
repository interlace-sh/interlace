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
