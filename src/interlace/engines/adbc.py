"""Shared base for ADBC-transport engines (Postgres, Redshift, Snowflake, BigQuery).

Everything an ADBC backend has in common lives here: canonical ASTs transpile to
the engine's dialect and run over one synchronous ADBC connection (behind a lock,
since a connection runs one statement at a time), results come back as Arrow, and
bulk loads go in via ``adbc_ingest`` — columnar end to end, no row-format hop. A
concrete engine is then just a ``dialect``, an :class:`EngineCaps`, and a
``connect`` that imports its driver.

``describe`` / ``table_exists`` default to ADBC's driver-agnostic metadata API
(``adbc_get_table_schema``); an engine with a cheaper or more precise probe (e.g.
Postgres' ``information_schema``) overrides them.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Sequence
from typing import Any

import pyarrow as pa
from sqlglot import exp

from interlace.engines.base import EngineAdapter, EngineCaps, LoadMode
from interlace.ir.relation import TableRef


def arrow_type_name(dtype: pa.DataType) -> str:
    """A canonical SQL type name for an Arrow type, in the vocabulary the planner's
    alignment/widening logic understands (see ``plan.apply``)."""
    if pa.types.is_boolean(dtype):
        return "BOOLEAN"
    if pa.types.is_int8(dtype):
        return "TINYINT"
    if pa.types.is_int16(dtype):
        return "SMALLINT"
    if pa.types.is_int32(dtype):
        return "INTEGER"
    if pa.types.is_integer(dtype):  # int64 + unsigned
        return "BIGINT"
    if pa.types.is_float32(dtype):
        return "FLOAT"
    if pa.types.is_floating(dtype):
        return "DOUBLE"
    if pa.types.is_decimal(dtype):
        return "DECIMAL"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return "BLOB"
    return "VARCHAR"


class AdbcAdapter(EngineAdapter):
    """Executes canonical ASTs inside an ADBC backend; Arrow in and out."""

    dialect: str = "postgres"
    caps: EngineCaps = EngineCaps()

    def __init__(self, connection: Any) -> None:  # an ADBC DBAPI Connection
        self._conn = connection
        self._lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    # --- EngineAdapter ------------------------------------------------------

    async def execute(self, ast: exp.Expression) -> None:
        await self.execute_sql(self.transpile(ast))

    async def execute_all(self, statements: Sequence[exp.Expression]) -> list[int]:
        return await asyncio.to_thread(self._execute_all_sync, [self.transpile(s) for s in statements])

    async def fetch(self, ast: exp.Expression) -> pa.RecordBatchReader:
        return await self.fetch_sql(self.transpile(ast))

    async def load(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> int:
        return await asyncio.to_thread(self._load_sync, table, reader, mode)

    async def create_view(self, name: TableRef, target: TableRef) -> None:
        await self.execute_sql(
            f"CREATE OR REPLACE VIEW {self._table_sql(name)} AS SELECT * FROM {self._table_sql(target)}"
        )

    async def create_schema(self, name: str) -> None:
        await self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {exp.to_identifier(name).sql(dialect=self.dialect)}")

    async def execute_sql(self, sql: str) -> None:
        await asyncio.to_thread(self._execute_sync, sql)

    async def fetch_sql(self, sql: str) -> pa.RecordBatchReader:
        return await asyncio.to_thread(self._fetch_sync, sql)

    async def table_exists(self, table: TableRef) -> bool:
        return bool(await self.describe(table))

    async def describe(self, table: TableRef) -> dict[str, str]:
        return await asyncio.to_thread(self._describe_sync, table)

    # --- sync workers (one connection; ADBC is synchronous) ------------------

    def _table_sql(self, table: TableRef) -> str:
        return table.to_expr().sql(dialect=self.dialect)

    def _execute_sync(self, sql: str) -> None:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(sql)
            self._conn.commit()

    def _execute_all_sync(self, sqls: list[str]) -> list[int]:
        counts: list[int] = []
        with self._lock:
            try:
                with self._conn.cursor() as cur:
                    for sql in sqls:
                        cur.execute(sql)
                        rowcount = getattr(cur, "rowcount", -1)
                        counts.append(rowcount if isinstance(rowcount, int) and rowcount > 0 else 0)
                self._conn.commit()  # ADBC autocommit is off: this is one transaction
            except Exception:
                self._conn.rollback()
                raise
        return counts

    def _fetch_sync(self, sql: str) -> pa.RecordBatchReader:
        # Materialised deliberately: one connection runs one query at a time, so a
        # lazily-streamed reader would hold the adapter lock for its whole lifetime —
        # and run_python_model opens all of a model's upstream handles before the
        # function runs, so two live fetches on one adapter would deadlock. Reading
        # the result fully under the lock keeps the reader connection-independent.
        with self._lock, self._conn.cursor() as cur:
            cur.execute(sql)
            table = cur.fetch_arrow_table()
            self._conn.commit()
        return table.to_reader()

    def _load_sync(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> int:
        ingest_mode = "replace" if mode == "create" else "append"  # "create" == CREATE OR REPLACE semantics
        with self._lock:
            try:
                with self._conn.cursor() as cur:
                    loaded = cur.adbc_ingest(table.name, reader, mode=ingest_mode, db_schema_name=table.schema)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return int(loaded) if isinstance(loaded, int) and loaded > 0 else 0

    def _describe_sync(self, table: TableRef) -> dict[str, str]:
        with self._lock:
            try:
                schema = self._conn.adbc_get_table_schema(table.name, db_schema_filter=table.schema)
            except Exception:  # driver raises when the table is absent -> treat as "no columns"
                return {}
        return {field.name: arrow_type_name(field.type) for field in schema}
