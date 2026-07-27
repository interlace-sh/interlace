"""Postgres engine adapter — the first native remote engine (ADBC transport).

Strategies execute *inside* Postgres: canonical ASTs transpile to the postgres
dialect and run over one ADBC connection; results come back as Arrow and bulk
loads go in via ``adbc_ingest`` — columnar end to end, no row-format hop.

Capability honesty drives the strategy fallbacks: Postgres has no
``CREATE OR REPLACE TABLE`` (FullRefresh falls back to DROP+CREATE) and no
star-EXCLUDE projection (scd_type_2 refuses with a clear error). ``merge_by_key``
and ``full_merge`` are portable by construction (DELETE+INSERT / set difference).

The ADBC connection is synchronous: calls run in a worker thread behind a lock
(one statement at a time per engine — remote engines parallelise internally).
Requires the ``adbc`` extra (``pip install 'interlace[adbc]'``).
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Sequence
from typing import Any

import pyarrow as pa
from sqlglot import exp

from interlace.engines.base import EngineAdapter, EngineCaps, LoadMode
from interlace.exceptions import ConfigurationError
from interlace.ir.relation import TableRef

_POSTGRES_CAPS = EngineCaps(
    supports_create_or_replace=False,  # no CREATE OR REPLACE TABLE -> DROP+CREATE fallback
    supports_star_exclude=False,  # no SELECT * EXCLUDE -> scd_type_2 unsupported for now
)


class PostgresAdapter(EngineAdapter):
    """Executes canonical ASTs inside Postgres; Arrow in and out via ADBC."""

    dialect = "postgres"
    caps = _POSTGRES_CAPS

    def __init__(self, connection: Any) -> None:  # adbc_driver_postgresql.dbapi.Connection
        self._conn = connection
        self._lock = threading.Lock()

    @classmethod
    def connect(cls, dsn: str) -> PostgresAdapter:
        try:
            import adbc_driver_postgresql.dbapi as dbapi  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - import guard
            raise ConfigurationError(
                "the postgres engine needs the 'adbc' extra: pip install 'interlace[adbc]'"
            ) from exc
        return cls(dbapi.connect(dsn))

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
        with self._lock, self._conn.cursor() as cur:
            cur.execute(sql)
            table = cur.fetch_arrow_table()  # materialised: keeps the reader cursor-independent
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

    def _table_exists_sync(self, table: TableRef) -> bool:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
                (table.schema, table.name),
            )
            row = cur.fetchone()
            self._conn.commit()
        return bool(row and row[0])

    def _describe_sync(self, table: TableRef) -> dict[str, str]:
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
                (table.schema, table.name),
            )
            rows = cur.fetchall()
            self._conn.commit()
        return dict(rows)
