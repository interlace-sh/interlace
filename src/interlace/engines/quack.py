"""Quack engine adapter — a remote DuckDB warehouse over the quack protocol.

Connects to a warehouse served by ``interlace serve --quack`` (or any
``CALL quack_serve(...)``). Every statement is shipped to the server via the
``quack_query`` table function — full SQL pass-through with results streamed
back as Arrow — because quack's catalog ATTACH only resolves the server's main
schema while the protocol is in beta. Arrow loads go through the attached
remote catalog (DDL resolves schema-qualified names correctly over the wire).
Multi-statement plans are sent as one BEGIN/COMMIT payload so they stay atomic
server-side. Requires DuckDB >= 1.5.3 (quack is a core extension there).
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
import pyarrow as pa
from sqlglot import exp

from interlace.engines.base import LoadMode
from interlace.engines.duckdb import DuckDBAdapter
from interlace.ir.relation import TableRef

_REMOTE = "__interlace_remote"


def sql_literal(value: str) -> str:
    return exp.Literal.string(value).sql(dialect="duckdb")


class QuackAdapter(DuckDBAdapter):
    """Executes canonical ASTs against a quack-served warehouse."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, uri: str) -> None:
        super().__init__(connection)
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
        cur = self._conn.cursor()
        try:
            self._remote_sync(cur, sql)
        finally:
            cur.close()

    def _execute_all_sync(self, sqls: list[str]) -> None:
        # One payload, transactional server-side; quack executes it as a unit.
        self._execute_sync(";\n".join(["BEGIN", *sqls, "COMMIT"]))

    def _fetch_sync(self, sql: str) -> pa.RecordBatchReader:
        cur = self._conn.cursor()
        return self._remote_sync(cur, sql).to_arrow_reader()

    def _load_sync(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> None:
        # Register the reader locally; schema-qualified DDL against the attached
        # remote catalog streams the batches over the wire. The attach caches the
        # remote catalog, so refresh it to see schemas created since we connected.
        cur = self._conn.cursor()
        cur.execute("CALL quack_clear_cache()")
        src = f"__interlace_src_{uuid4().hex}"
        cur.register(src, reader)
        try:
            target = exp.table_(table.name, db=table.schema, catalog=_REMOTE).sql(dialect=self.dialect)
            if mode == "create":
                cur.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {src}")
            else:
                cur.execute(f"INSERT INTO {target} SELECT * FROM {src}")
        finally:
            cur.unregister(src)
            cur.close()

    def _table_exists_sync(self, table: TableRef) -> bool:
        sql = (
            "SELECT count(*) FROM information_schema.tables "
            f"WHERE table_schema = {sql_literal(table.schema)} AND table_name = {sql_literal(table.name)}"
        )
        cur = self._conn.cursor()
        try:
            row = self._remote_sync(cur, sql).fetchone()
        finally:
            cur.close()
        return bool(row and row[0])

    def _describe_sync(self, table: TableRef) -> dict[str, str]:
        sql = (
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = {sql_literal(table.schema)} AND table_name = {sql_literal(table.name)} "
            "ORDER BY ordinal_position"
        )
        cur = self._conn.cursor()
        try:
            rows = self._remote_sync(cur, sql).fetchall()
        finally:
            cur.close()
        return dict(rows)
