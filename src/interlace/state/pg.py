"""Postgres control-plane connection.

Stores keep writing SQLite-shaped SQL. This wrapper translates the dialect
gaps (placeholders, ``INSERT OR IGNORE`` / ``INSERT OR REPLACE``, identity
columns, ``json_extract``) and returns rows that support both ``row["col"]`` and ``row[0]``.
"""

from __future__ import annotations

import re
import zlib
from collections.abc import Sequence
from typing import Any

from interlace.state.schema import _MIGRATIONS

STATE_SCHEMA = "interlace"
STREAM_SCHEMA = "interlace_streams"

_SCHEMA_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_OR_REPLACE = re.compile(
    r"INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)",
    re.IGNORECASE,
)
_OR_IGNORE = re.compile(r"INSERT\s+OR\s+IGNORE\s+INTO", re.IGNORECASE)
_JSON_EXTRACT = re.compile(r"json_extract\(\s*([^,]+?)\s*,\s*'\$\.([^']+)'\s*\)", re.IGNORECASE)
# Primary keys for INSERT OR REPLACE. ON CONFLICT needs them; SQLite inferred them.
_REPLACE_KEYS: dict[str, tuple[str, ...]] = {
    "snapshots": ("name", "fingerprint"),
    "environments": ("environment", "model_name"),
    "trigger_state": ("trigger_id",),
    "cdc_confirmed": ("stream",),
    "cdc_pending": ("stream", "log_offset"),
    "promotion_history": ("environment", "generation", "model_name"),
    "stream_heads": ("stream",),
}


class PgRow(dict[str, Any]):
    """A result row. Column order is the integer index, names are the keys."""

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class PgCursor:
    def __init__(self, raw: Any, lastrowid: int, *, drained: bool) -> None:
        self._raw = raw
        self.lastrowid = lastrowid
        self.rowcount = -1 if raw is None else int(raw.rowcount)
        self._drained = drained

    def fetchone(self) -> PgRow | None:
        if self._drained or self._raw is None:
            return None
        row = self._raw.fetchone()
        return PgRow(row) if row is not None else None

    def fetchall(self) -> Sequence[PgRow]:
        if self._drained or self._raw is None:
            return []
        return [PgRow(row) for row in self._raw.fetchall()]


class PgConn:
    """A psycopg connection with a sqlite3-shaped ``execute``."""

    dialect = "postgres"

    def __init__(self, raw: Any) -> None:
        self._raw = raw

    def execute(self, sql: str, parameters: Any = None) -> PgCursor:
        text = translate(sql).strip()
        upper = text.upper()
        if upper == "ROLLBACK":
            self.rollback()
            return PgCursor(None, 0, drained=True)
        if upper == "COMMIT":
            self.commit()
            return PgCursor(None, 0, drained=True)
        returning = upper.startswith("INSERT INTO EVENT_LOG ")
        if returning:
            text = text.rstrip(";") + " RETURNING seq"
        raw = self._raw.execute(text) if parameters is None else self._raw.execute(text, parameters)
        lastrowid = 0
        if returning:
            row = raw.fetchone()
            if row is not None:
                lastrowid = int(row["seq"])
        return PgCursor(raw, lastrowid, drained=returning)

    def executemany(self, sql: str, seq_of_parameters: Any) -> PgCursor:
        rows = list(seq_of_parameters)
        if not rows:
            return PgCursor(None, 0, drained=True)
        raw = self._raw.executemany(translate(sql), rows)
        return PgCursor(raw, 0, drained=True)

    def executescript(self, script: str) -> None:
        for statement in script.split(";"):
            if statement.strip():
                self.execute(statement)

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        self._raw.close()


def translate(sql: str) -> str:
    """SQLite statement → Postgres. Placeholders become ``%s``."""
    text = sql.strip()
    if re.search(r"INSERT\s+OR\s+REPLACE", text, re.IGNORECASE):
        match = _OR_REPLACE.search(text)
        if match is None:
            raise ValueError("INSERT OR REPLACE needs a column list")
        table = match.group(1)
        keys = _REPLACE_KEYS.get(table.lower())
        if keys is None:
            raise ValueError(f"INSERT OR REPLACE into unknown table {table}")
        columns = [column.strip() for column in match.group(2).split(",")]
        updates = [column for column in columns if column not in keys]
        assignment = ", ".join(f"{column} = EXCLUDED.{column}" for column in updates)
        if not assignment:
            assignment = f"{keys[0]} = EXCLUDED.{keys[0]}"
        text = _OR_REPLACE.sub(f"INSERT INTO {table} ({match.group(2)})", text, count=1)
        text = text.rstrip().rstrip(";") + f" ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {assignment}"
    elif _OR_IGNORE.search(text):
        text = _OR_IGNORE.sub("INSERT INTO", text, count=1)
        text = text.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    text = re.sub(r"\bBEGIN\s+IMMEDIATE\b", "BEGIN", text, flags=re.IGNORECASE)
    text = text.replace(
        "INTEGER PRIMARY KEY AUTOINCREMENT",
        "BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY",
    )
    # ->> yields text. Callers that compare a JSON number cast the extract to text
    # and bind the value as text, so SQLite and Postgres agree.
    text = _JSON_EXTRACT.sub(r"(\1::json ->> '\2')", text)
    return text.replace("?", "%s")


def check_schema(name: str) -> str:
    if not _SCHEMA_NAME.fullmatch(name):
        raise ValueError(f"schema name {name!r} must be a lowercase identifier")
    return name


def connect(dsn: str, schema: str) -> PgConn:
    """Open a Postgres connection pinned to ``schema``, creating the schema."""
    import psycopg
    from psycopg.rows import dict_row

    name = check_schema(schema)
    raw = psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=10)
    conn = PgConn(raw)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {name}")
    conn.execute(f"SET search_path TO {name}")
    return conn


def migrate(conn: PgConn, schema: str) -> None:
    """Apply control-plane migrations. Version lives in ``schema_migrations``, not ``PRAGMA``."""
    name = check_schema(schema)
    lock_key = zlib.crc32(f"interlace-state:{name}".encode())
    conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)")
    conn.execute("SELECT pg_advisory_lock(?)", (lock_key,))
    try:
        while True:
            row = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()
            version = int(row[0]) if row is not None else 0
            if version >= len(_MIGRATIONS):
                return
            conn.execute("BEGIN")
            try:
                for statement in _MIGRATIONS[version].split(";"):
                    if statement.strip():
                        conn.execute(statement)
                conn.execute("INSERT INTO schema_migrations (version) VALUES (?)", (version + 1,))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    finally:
        conn.execute("SELECT pg_advisory_unlock(?)", (lock_key,))
