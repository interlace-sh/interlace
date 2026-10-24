"""Quack engine adapter — a remote DuckDB warehouse over the quack protocol.

Connects to a warehouse served by ``interlace serve --quack`` (or any
``CALL quack_serve(...)``). Every statement is shipped to the server via the
``quack_query`` table function, and a multi-statement plan is one
``BEGIN``/``COMMIT`` payload so the server applies it as a single transaction.
``ATTACH`` resolves schema-qualified names outside ``main``; loads use that
attached catalog. Queries stay on ``quack_query`` so the transactional payload
has one place to live. quack is a core extension.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from uuid import uuid4

import duckdb
import pyarrow as pa
from sqlglot import exp

from interlace.engines.base import LoadMode, note_statement
from interlace.engines.duckdb import _DUCKDB_CAPS, DuckDBAdapter
from interlace.ir.relation import TableRef

_QUACK_CAPS = replace(_DUCKDB_CAPS, supports_attach=False)

_REMOTE = "__interlace_remote"


def sql_literal(value: str) -> str:
    return exp.Literal.string(value).sql(dialect="duckdb")


class QuackAdapter(DuckDBAdapter):
    """Executes canonical ASTs against a quack-served warehouse."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, uri: str) -> None:
        super().__init__(connection)
        self.caps = _QUACK_CAPS  # SQL runs remotely; this process cannot ATTACH the source
        self._uri = uri

    @classmethod
    def connect(cls, path: str, token: str | None = None) -> QuackAdapter:
        """Connect to a ``quack:<host>:<port>`` warehouse; ``token`` is the serve token."""
        conn = duckdb.connect(":memory:")
        if token:
            conn.execute(f"CREATE SECRET {_REMOTE}_secret (TYPE quack, TOKEN {sql_literal(token)})")
        conn.execute(f"ATTACH {sql_literal(path)} AS {_REMOTE}")  # used for Arrow loads; also validates reachability
        return cls(conn, path)

    # --- sync workers: route SQL through quack_query -------------------------

    def _remote_sync(self, cur: duckdb.DuckDBPyConnection, sql: str) -> duckdb.DuckDBPyConnection:
        return cur.execute("FROM quack_query(?, ?)", [self._uri, sql])

    def _execute_sync(self, sql: str) -> None:
        cur = self._cursor()
        try:
            self._remote_sync(cur, sql)
        except Exception as exc:
            note_statement(exc, sql)
            raise
        finally:
            self._close_cursor(cur)

    async def commit_batch(self, source: str, reader: pa.RecordBatchReader, statements: Sequence[exp.Expr]) -> bool:
        # The Arrow register is local and the statements run on the server.
        # They are not one transaction, so the caller keeps the stage table.
        del source, reader, statements
        return False

    def _execute_all_sync(self, sqls: list[str]) -> list[int]:
        # One payload, transactional server-side; quack executes it as a unit —
        # per-statement affected counts are not observable over the wire.
        self._execute_sync(";\n".join(["BEGIN", *sqls, "COMMIT"]))
        return [0] * len(sqls)

    def _fetch_sync(self, sql: str, *, sandboxed: bool = False) -> pa.RecordBatchReader:
        # `sandboxed` can't be honoured over the wire (the query runs on the remote
        # quack server, its own trust boundary); the console's parse-time deny-list
        # is the fence for quack-backed warehouses.
        cur = self._cursor()
        try:
            # A relation streams. execute().to_arrow_reader() copies the rows and warns.
            reader = cur.sql("FROM quack_query(?, ?)", params=[self._uri, sql]).to_arrow_reader()
        except Exception:
            self._close_cursor(cur)
            raise
        return self._stream_reader(cur, reader)

    def _load_sync(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> int:
        # Register the reader locally; schema-qualified DDL against the attached
        # remote catalog streams the batches over the wire. The attach caches the
        # remote catalog, so refresh it to see schemas created since we connected.
        cur = self._cursor()
        try:
            cur.execute("CALL quack_clear_cache()")
            src = f"__interlace_src_{uuid4().hex}"
            cur.register(src, reader)
            try:
                target = exp.table_(table.name, db=table.schema, catalog=_REMOTE).sql(dialect=self.dialect)
                if mode == "create":
                    cur.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {src}")
                else:
                    cur.execute(f"INSERT INTO {target} SELECT * FROM {src}")
                rows = cur.fetchall()
                return int(rows[0][0]) if rows and rows[0] and isinstance(rows[0][0], int) else 0
            finally:
                cur.unregister(src)
        finally:
            self._close_cursor(cur)

    def _table_exists_sync(self, table: TableRef) -> bool:
        sql = (
            "SELECT count(*) FROM information_schema.tables "
            f"WHERE table_schema = {sql_literal(table.schema)} AND table_name = {sql_literal(table.name)}"
        )
        cur = self._cursor()
        try:
            row = self._remote_sync(cur, sql).fetchone()
        finally:
            self._close_cursor(cur)
        return bool(row and row[0])

    def _describe_sync(self, table: TableRef) -> dict[str, str]:
        sql = (
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = {sql_literal(table.schema)} AND table_name = {sql_literal(table.name)} "
            "ORDER BY ordinal_position"
        )
        cur = self._cursor()
        try:
            rows = self._remote_sync(cur, sql).fetchall()
        finally:
            self._close_cursor(cur)
        return dict(rows)
