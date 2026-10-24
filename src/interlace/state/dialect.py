"""Control-plane statements that are not the same SQL on SQLite and Postgres.

:func:`interlace.state.pg.translate` rewrites placeholders, ``BEGIN IMMEDIATE``,
and ``AUTOINCREMENT`` for the statements that otherwise match. An upsert, an
insert-ignore, and a JSON field read do not match, so they are written here
instead of being recovered from SQLite text with a regular expression.
"""

from __future__ import annotations

from collections.abc import Sequence


def upsert(table: str, columns: Sequence[str], keys: Sequence[str], *, dialect: str) -> str:
    """Insert or replace the row identified by ``keys``."""
    cols = ", ".join(columns)
    marks = ", ".join("?" for _ in columns)
    if dialect != "postgres":
        return f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({marks})"
    updates = [column for column in columns if column not in keys]
    assignment = ", ".join(f"{column} = EXCLUDED.{column}" for column in updates)
    if not assignment:
        assignment = f"{keys[0]} = EXCLUDED.{keys[0]}"
    conflict = ", ".join(keys)
    return f"INSERT INTO {table} ({cols}) VALUES ({marks}) ON CONFLICT ({conflict}) DO UPDATE SET {assignment}"


def insert_ignore(table: str, columns: Sequence[str], *, dialect: str) -> str:
    """Insert the row, or leave the existing one when a unique key collides."""
    cols = ", ".join(columns)
    marks = ", ".join("?" for _ in columns)
    if dialect != "postgres":
        return f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({marks})"
    return f"INSERT INTO {table} ({cols}) VALUES ({marks}) ON CONFLICT DO NOTHING"


def json_field(column: str, key: str, *, dialect: str) -> str:
    """The text of one field of a JSON object column."""
    if not key.isidentifier():
        raise ValueError(f"json field {key!r} must be an identifier")
    if dialect == "postgres":
        return f"({column}::json ->> '{key}')"
    return f"json_extract({column}, '$.{key}')"
