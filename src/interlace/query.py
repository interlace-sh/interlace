"""Read-only query preparation, shared by the HTTP console and `interlace query`.

Parses a single ``SELECT`` and fences it: table sources must be real tables/views
or a vetted row generator (``range``/``generate_series``) — never a table function
(``read_csv`` / ``read_parquet`` / ``query`` / ``query_table`` / ``glob`` / …, named
or not), never a DuckDB path-as-table (``FROM 'file.csv'``), never ``pragma_*`` /
``duckdb_*`` system relations, with a file/network function-name backstop. So no
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
    r"^(read_|scan_|sniff_|glob$|getenv$|load_|install$|parquet_|iceberg_|delta_|st_read|"
    r"query$|query_table$|http_|curl$|wget$|fetch_)"
    r"|(_scan|_query)$",
    re.IGNORECASE,
)
# DuckDB (and friends) treat FROM 'file.csv' / FROM "data.parquet" as a file scan.
# Those parse as quoted Identifiers — not table functions — so the allowlist alone
# cannot catch them.
_FILE_EXT = re.compile(
    r"\.(csv|tsv|txt|parquet|json|jsonl|ndjson|gz|gzip|xlsx|xls|orc|avro|arrow|ipc|"
    r"duckdb|db|csv\.gz|json\.gz|parquet\.gz)$",
    re.IGNORECASE,
)
# Warehouse system catalogs / pragmas that leak host paths or bypass the fence.
_SYSTEM_RELATION = re.compile(r"^(pragma_|duckdb_)", re.IGNORECASE)
# Table functions the console may use: pure in-memory row generators, no I/O.
# `range(...)` normalises to generate_series in the sqlglot AST.
_SAFE_TABLE_FUNCTIONS = frozenset({"generate_series"})


def _fn_name(node: exp.Expr) -> str | None:
    if isinstance(node, exp.Anonymous):
        return str(node.this).lower()
    if isinstance(node, exp.Func):
        return str(node.sql_name()).lower()
    return None


def _reject_file_like_identifier(table: exp.Table) -> None:
    """Reject DuckDB path-as-table forms (``FROM 'x.csv'``) and system relations."""
    raw = table.name or ""
    if "/" in raw or "\\" in raw or "://" in raw:
        raise QueryError("file paths are not queryable — read tables, not files")
    if _SYSTEM_RELATION.match(raw):
        raise QueryError(f"system relation {raw!r} is not queryable from the console")
    ident = table.this
    quoted = isinstance(ident, exp.Identifier) and bool(getattr(ident, "quoted", False))
    # Quoted names with a file extension (or a bare relative path like './x') are
    # DuckDB's file-scan syntax. Unquoted ``file.csv`` parses as db.table, which is fine.
    if quoted and (_FILE_EXT.search(raw) or raw.startswith(".") or "." in raw):
        raise QueryError("file paths are not queryable — read tables, not files")


def guard_readonly(parsed: exp.Expr) -> None:
    """Reject anything a read-only query must never do — table functions, file paths,
    and file/network readers. Raises :class:`QueryError`."""
    for table in parsed.find_all(exp.Table):
        if isinstance(table.this, exp.Identifier):  # a real table/view reference
            _reject_file_like_identifier(table)
            continue
        name = _fn_name(table.this)  # a table function in FROM/JOIN position
        if name not in _SAFE_TABLE_FUNCTIONS:
            raise QueryError(f"table function {name or 'call'!r} is not allowed — read tables only")
    for node in parsed.walk():  # backstop: file/network readers in scalar / LATERAL position
        name = _fn_name(node)
        if name and _EXTERNAL_FN.search(name):
            raise QueryError(f"function {name!r} reads outside the warehouse — not allowed")


def prepare_readonly(sql: str, dialect: str, limit: int) -> tuple[exp.Expr, int]:
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
