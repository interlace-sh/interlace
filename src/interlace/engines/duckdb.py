"""DuckDB engine adapter — the default local engine and federation hub.

Everything crosses the boundary as Arrow: :meth:`fetch` streams results as a
``pyarrow.RecordBatchReader`` (zero-copy, single pass) and :meth:`load` registers
an Arrow reader and writes it with one ``CREATE TABLE AS`` / ``INSERT``. Blocking
DuckDB calls run in a worker thread; each call uses its own ``cursor()`` so reads
proceed concurrently (DuckDB MVCC), while the DAG guarantees no two tasks write
the same table at once.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from uuid import uuid4

import duckdb
import pyarrow as pa
from sqlglot import exp

from interlace.engines.base import EngineAdapter, EngineCaps, LoadMode
from interlace.ir.relation import TableRef

_DUCKDB_CAPS = EngineCaps(
    supports_merge=True,  # MERGE INTO verified on DuckDB >= 1.3
    supports_clone=False,
    supports_qualify=True,
    supports_create_or_replace=True,
    supports_arrow_ingest=True,
    supports_attach=True,
)


class DuckDBAdapter(EngineAdapter):
    """Executes canonical ASTs and moves Arrow data in and out of a DuckDB database."""

    dialect = "duckdb"
    caps = _DUCKDB_CAPS

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self._conn = connection

    @classmethod
    def in_memory(cls) -> DuckDBAdapter:
        return cls(duckdb.connect(":memory:"))

    @classmethod
    def connect(cls, path: str) -> DuckDBAdapter:
        return cls(duckdb.connect(path))

    def close(self) -> None:
        self._conn.close()

    # --- identifier helpers -------------------------------------------------

    def _table_sql(self, table: TableRef) -> str:
        return exp.table_(table.name, db=table.schema, catalog=table.catalog).sql(dialect=self.dialect)

    # --- EngineAdapter ------------------------------------------------------

    async def execute(self, ast: exp.Expression) -> None:
        await self.execute_sql(self.transpile(ast))

    async def execute_all(self, statements: Sequence[exp.Expression]) -> None:
        await asyncio.to_thread(self._execute_all_sync, [self.transpile(s) for s in statements])

    async def fetch(self, ast: exp.Expression) -> pa.RecordBatchReader:
        return await self.fetch_sql(self.transpile(ast))

    async def load(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> None:
        await asyncio.to_thread(self._load_sync, table, reader, mode)

    async def create_view(self, name: TableRef, target: TableRef) -> None:
        await self.execute_sql(
            f"CREATE OR REPLACE VIEW {self._table_sql(name)} AS SELECT * FROM {self._table_sql(target)}"
        )

    # --- raw / convenience (used by the state store and tests) --------------

    async def execute_sql(self, sql: str) -> None:
        await asyncio.to_thread(self._execute_sync, sql)

    async def fetch_sql(self, sql: str) -> pa.RecordBatchReader:
        return await asyncio.to_thread(self._fetch_sync, sql)

    async def create_schema(self, name: str) -> None:
        await self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {exp.to_identifier(name).sql(dialect=self.dialect)}")

    async def table_exists(self, table: TableRef) -> bool:
        return await asyncio.to_thread(self._table_exists_sync, table)

    async def describe(self, table: TableRef) -> dict[str, str]:
        return await asyncio.to_thread(self._describe_sync, table)

    # --- sync workers (run in a thread) -------------------------------------

    def _execute_sync(self, sql: str) -> None:
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def _execute_all_sync(self, sqls: list[str]) -> None:
        cur = self._conn.cursor()
        try:
            cur.execute("BEGIN")
            for sql in sqls:
                cur.execute(sql)
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.close()

    def _fetch_sync(self, sql: str) -> pa.RecordBatchReader:
        # The reader keeps the underlying result alive after the cursor is dropped.
        cur = self._conn.cursor()
        cur.execute(sql)
        return cur.to_arrow_reader()

    def _load_sync(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> None:
        cur = self._conn.cursor()
        src = f"__interlace_src_{uuid4().hex}"
        cur.register(src, reader)
        try:
            target = self._table_sql(table)
            if mode == "create":
                cur.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {src}")
            else:
                cur.execute(f"INSERT INTO {target} SELECT * FROM {src}")
        finally:
            cur.unregister(src)
            cur.close()

    def _table_exists_sync(self, table: TableRef) -> bool:
        cur = self._conn.cursor()
        try:
            row = cur.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                [table.schema, table.name],
            ).fetchone()
        finally:
            cur.close()
        return bool(row and row[0])

    def _describe_sync(self, table: TableRef) -> dict[str, str]:
        cur = self._conn.cursor()
        try:
            rows = cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                [table.schema, table.name],
            ).fetchall()
        finally:
            cur.close()
        return dict(rows)
