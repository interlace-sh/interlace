"""Where a DuckLake CREATE actually landed.

A sibling cursor during COMMIT can record an empty schema name and store the
new table in ``main``. DuckLake connections pin a CREATE to
``catalog.schema.table`` and then confirm the relation is in the schema the
statement named. A miss rolls back and retries. A leftover base table in
``main`` beside one that landed correctly is dropped. Views are never dropped:
another environment's view shares the model's short name.

Plain DuckDB does not call this. The bug is the DuckLake catalog.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal, NamedTuple

import duckdb
import sqlglot
from sqlglot import exp

Kind = Literal["TABLE", "VIEW"]
_ATTEMPTS = 3


class CatalogPlacementError(Exception):
    """DuckLake committed a CREATE under a schema other than the one the statement named.

    A plain Exception, not an :class:`~interlace.exceptions.InterlaceError`: the scheduler
    wraps those as ``model '…' failed: …`` and does not record the snapshot.
    """


class Created(NamedTuple):
    schema: str
    name: str
    kind: Kind


def created_relations(sql: str) -> list[Created]:
    """CREATEs that name a schema. Empty when the statement is not one, or names no schema.

    An unqualified create cannot be told apart from the DuckLake bug that drops the schema.
    """
    try:
        expression = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return []
    if not isinstance(expression, exp.Create):
        return []
    kind = _create_kind(expression)
    if kind is None:
        return []
    target = expression.this
    if isinstance(target, exp.Schema):
        target = target.this
    if not isinstance(target, exp.Table) or not target.db or not target.name:
        return []
    return [Created(target.db, target.name, kind)]


def pin_catalog(sql: str, alias: str | None) -> str:
    """Write an attached DuckLake CREATE as ``catalog.schema.table``.

    A two-part ``schema.table`` is resolved in the session's default schema when
    DuckLake drops the qualifier. The catalog alias makes the schema part of the
    name DuckLake commits. No alias means the statement is unchanged.
    """
    if not alias:
        return sql
    try:
        expression = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return sql
    if not isinstance(expression, exp.Create):
        return sql
    target = expression.this
    if isinstance(target, exp.Schema):
        target = target.this
    if not isinstance(target, exp.Table) or not target.db or target.catalog:
        return sql
    target.set("catalog", exp.to_identifier(alias))
    return expression.sql(dialect="duckdb")


def confirm_statement(cur: duckdb.DuckDBPyConnection, sql: str) -> None:
    for created in created_relations(sql):
        confirm_placed(cur, created.schema, created.name, kind=created.kind)


def confirm_placed(cur: duckdb.DuckDBPyConnection, schema: str, name: str, *, kind: Kind) -> None:
    """Reject a CREATE whose schema name did not survive.

    A leftover *table* already in ``main`` beside one that did land in the named
    schema is the previous occurrence of this bug and is dropped. Views are left
    alone. ``kind`` is required: a default of TABLE is what made a view promotion
    look like a misplaced table.
    """
    if not schema or schema.casefold() == "main":
        return
    found = _placed_schemas(cur, name, kind=kind)
    if any(placed.casefold() == schema.casefold() for placed in found):
        drop_main_copy(cur, name, kind=kind)
        return
    if not found:
        return
    where = ", ".join(sorted(found))
    raise CatalogPlacementError(f"DuckLake created {schema}.{name} in schema {where} with an empty schema name")


def drop_misplaced(cur: duckdb.DuckDBPyConnection, sql: str) -> None:
    """Drop a table copy in ``main`` left by an autocommitted CREATE.

    A transaction rolls back instead of calling this. Views are never dropped.
    """
    for created in created_relations(sql):
        drop_main_copy(cur, created.name, kind=created.kind)


def drop_main_copy(cur: duckdb.DuckDBPyConnection, name: str, *, kind: Kind) -> None:
    if kind != "TABLE":
        return
    for placed in _placed_schemas(cur, name, kind=kind):
        if placed.casefold() == "main":
            cur.execute(f"DROP TABLE IF EXISTS {_qualified(placed, name)}")


def retry_placement[T](operation: Callable[[], T]) -> T:
    """Run ``operation`` up to three times when DuckLake files the CREATE in the wrong schema."""
    last: CatalogPlacementError | None = None
    for _attempt in range(_ATTEMPTS):
        try:
            return operation()
        except CatalogPlacementError as exc:
            last = exc
    assert last is not None
    raise last


def _create_kind(expression: exp.Create) -> Kind | None:
    raw = str(expression.args.get("kind") or "").upper()
    if raw == "TABLE":
        return "TABLE"
    if raw == "VIEW":
        return "VIEW"
    return None


def _placed_schemas(cur: duckdb.DuckDBPyConnection, name: str, *, kind: Kind) -> set[str]:
    """Schemas in the current catalog that hold this table, or this view — not both."""
    table_type = "VIEW" if kind == "VIEW" else "BASE TABLE"
    rows = cur.execute(
        "SELECT table_schema FROM information_schema.tables "
        "WHERE table_name = ? AND table_type = ? AND table_catalog = current_database()",
        [name, table_type],
    ).fetchall()
    return {str(row[0]) for row in rows if row and row[0]}


def _qualified(schema: str, name: str) -> str:
    return f"{exp.to_identifier(schema).sql(dialect='duckdb')}.{exp.to_identifier(name).sql(dialect='duckdb')}"
