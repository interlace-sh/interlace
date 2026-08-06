"""Read-only query preparation, shared by the HTTP console and `interlace query`.

Parses a single ``SELECT`` and fences it: table sources must be real tables/views
or a vetted row generator (``range``/``generate_series``) — never a table function
(``read_csv`` / ``read_parquet`` / ``query`` / ``query_table`` / ``glob`` / …, named
or not) — with a file/network function-name backstop and a file-path check. So no
query can reach outside the warehouse. This parse-time fence is the security
boundary: DuckDB's engine-level lockdown (``enable_external_access``) is instance-
wide and one-way, so flipping it on the shared warehouse connection would break the
writer's own file writes (see docs/engines.md). The prepared query is wrapped with a
row cap (+1, so the caller can detect truncation).
"""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

from interlace.exceptions import QueryError

MAX_ROWS = 10_000

_EXTERNAL_FN = re.compile(
    r"^(read_|scan_|sniff_|glob$|getenv$|load_|install$|parquet_|iceberg_|delta_|st_read|query$|query_table$)"
    r"|(_scan|_query)$",
    re.IGNORECASE,
)
# Table functions the console may use: pure in-memory row generators, no I/O.
# `range(...)` normalises to generate_series in the sqlglot AST.
_SAFE_TABLE_FUNCTIONS = frozenset({"generate_series"})


def _fn_name(node: exp.Expression) -> str | None:
    if isinstance(node, exp.Anonymous):
        return str(node.this).lower()
    if isinstance(node, exp.Func):
        return str(node.sql_name()).lower()
    return None


def guard_readonly(parsed: exp.Expression) -> None:
    """Reject anything a read-only query must never do — table functions, file paths,
    and file/network readers. Raises :class:`QueryError`."""
    for table in parsed.find_all(exp.Table):
        if isinstance(table.this, exp.Identifier):  # a real table/view reference
            raw = table.name or ""
            if "/" in raw or "\\" in raw or "://" in raw:
                raise QueryError("file paths are not queryable — read tables, not files")
            continue
        name = _fn_name(table.this)  # a table function in FROM/JOIN position
        if name not in _SAFE_TABLE_FUNCTIONS:
            raise QueryError(f"table function {name or 'call'!r} is not allowed — read tables only")
    for node in parsed.walk():  # backstop: file/network readers in scalar position
        name = _fn_name(node)
        if name and _EXTERNAL_FN.search(name):
            raise QueryError(f"function {name!r} reads outside the warehouse — not allowed")


def prepare_readonly(sql: str, dialect: str, limit: int) -> tuple[exp.Expression, int]:
    """Parse and fence one read-only ``SELECT``; return (bounded AST, effective row cap).

    The AST is ``SELECT * FROM (<query>) LIMIT cap + 1`` so a caller can tell a full
    page from a truncated one. Raises :class:`QueryError` for anything that isn't a
    single fenced SELECT."""
    try:
        statements = sqlglot.parse(sql, read=dialect)
    except Exception as exc:
        raise QueryError(f"could not parse query: {exc}") from exc
    if len(statements) != 1 or statements[0] is None:
        raise QueryError("exactly one statement, please")
    parsed = statements[0]
    if not isinstance(parsed, (exp.Select, exp.Union)):
        raise QueryError("SELECT only — this reads, it never writes")
    guard_readonly(parsed)
    cap = max(1, min(limit, MAX_ROWS))
    return exp.select("*").from_(parsed.subquery("q")).limit(cap + 1), cap
