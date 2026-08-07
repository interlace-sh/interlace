"""Spark engine adapter — canonical ASTs run inside a Spark session.

Unlike the ADBC engines, Spark's transport is PySpark's ``SparkSession`` directly:
``spark.sql`` executes canonical ASTs transpiled to the Spark dialect, results
come back as Arrow via ``DataFrame.toArrow()``, and Arrow loads go in through
``spark.createDataFrame`` — no row-format hop. Works with a local session
(``local[*]``, for tests) or a remote one (Spark Connect / a shared session).

**Strategy support.** ``replace``, ``append`` and ``view`` run on any Spark
catalog. ``merge`` (native ``MERGE``) and ``incremental`` (windowed
``DELETE`` by literal predicate + ``INSERT``) need a catalog with row-level
mutations — Delta Lake or Iceberg — configured on the session you hand the
adapter (the tests use a Delta-backed local session). ``scd`` and ``full_merge``
are **not supported on Spark**: their close/delete conditions use a subquery
(``key IN (SELECT ...)``), and Delta rejects subqueries in ``UPDATE``/``DELETE``
conditions (``DELTA_UNSUPPORTED_SUBQUERY``) — they'd need a MERGE-based rewrite.

``execute_all`` is *not* one transaction — Spark has no multi-statement
transactions, so a strategy's statements run in sequence and a mid-sequence
failure can leave partial state. Affected-row counts aren't surfaced (reported
as 0); ``load`` reports its Arrow row count. Needs the ``spark`` extra.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from sqlglot import exp

from interlace.engines.base import EngineAdapter, EngineCaps, LoadMode
from interlace.exceptions import ConfigurationError
from interlace.ir.relation import TableRef

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_SPARK_CAPS = EngineCaps(
    supports_create_or_replace=False,  # portable across catalogs -> DROP + CREATE TABLE AS
    supports_star_exclude=False,  # no portable SELECT * EXCLUDE -> scd enumerates the model's columns
    supports_merge=True,  # native MERGE (on a Delta/Iceberg catalog)
)

# Spark type simpleString -> the planner's alignment/widening vocabulary (see plan.apply).
_SPARK_TYPES = {
    "boolean": "BOOLEAN",
    "tinyint": "TINYINT",
    "byte": "TINYINT",
    "smallint": "SMALLINT",
    "short": "SMALLINT",
    "int": "INTEGER",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "long": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamp_ntz": "TIMESTAMP",
    "binary": "BLOB",
}


def spark_type_name(simple_string: str) -> str:
    """Map a Spark ``dataType.simpleString()`` to a canonical SQL type name."""
    base = simple_string.strip().lower()
    if base.startswith("decimal"):
        return "DECIMAL"
    return _SPARK_TYPES.get(base, "VARCHAR")


class SparkAdapter(EngineAdapter):
    """Executes canonical ASTs inside a Spark session; Arrow in and out."""

    dialect = "spark"
    caps = _SPARK_CAPS

    def __init__(self, session: SparkSession) -> None:
        self._spark = session

    @classmethod
    def connect(cls, master: str) -> SparkAdapter:
        """Open a session from a master URL (``local[*]``, or ``sc://host:port`` for
        Spark Connect). Catalog/format config (Delta, Iceberg) is the session's own —
        set it via ``SPARK_CONF``/a preconfigured session for mutating strategies."""
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:  # pragma: no cover - import guard
            raise ConfigurationError(
                "the spark engine needs the 'spark' extra: pip install 'interlaced[spark]'"
            ) from exc
        builder = SparkSession.builder
        session = builder.remote(master) if master.startswith("sc://") else builder.master(master or "local[*]")
        return cls(session.getOrCreate())

    def close(self) -> None:
        self._spark.stop()

    # --- EngineAdapter ------------------------------------------------------

    async def execute(self, ast: exp.Expression) -> None:
        await self.execute_sql(self.transpile(ast))

    async def execute_all(self, statements: Sequence[exp.Expression]) -> list[int]:
        sqls = [self.transpile(s) for s in statements]
        await asyncio.to_thread(self._run_all_sync, sqls)
        return [0] * len(sqls)  # Spark doesn't surface affected-row counts

    async def fetch(self, ast: exp.Expression) -> pa.RecordBatchReader:
        return await self.fetch_sql(self.transpile(ast))

    async def load(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> int:
        return await asyncio.to_thread(self._load_sync, table, reader, mode)

    async def create_view(self, name: TableRef, target: TableRef) -> None:
        await self.execute_sql(f"CREATE OR REPLACE VIEW {self._fqtn(name)} AS SELECT * FROM {self._fqtn(target)}")

    async def create_schema(self, name: str) -> None:
        await self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {exp.to_identifier(name).sql(dialect=self.dialect)}")

    async def execute_sql(self, sql: str) -> None:
        await asyncio.to_thread(self._spark.sql, sql)

    async def fetch_sql(self, sql: str) -> pa.RecordBatchReader:
        return await asyncio.to_thread(self._fetch_sync, sql)

    async def table_exists(self, table: TableRef) -> bool:
        return await asyncio.to_thread(self._spark.catalog.tableExists, self._fqtn(table))

    async def describe(self, table: TableRef) -> dict[str, str]:
        return await asyncio.to_thread(self._describe_sync, table)

    # --- sync workers -------------------------------------------------------

    def _fqtn(self, table: TableRef) -> str:
        return table.to_expr().sql(dialect=self.dialect)

    def _run_all_sync(self, sqls: list[str]) -> None:
        for sql in sqls:
            self._spark.sql(sql)

    def _fetch_sync(self, sql: str) -> pa.RecordBatchReader:
        table = self._spark.sql(sql).toArrow()
        return table.to_reader()

    def _load_sync(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> int:
        arrow_table = reader.read_all()
        frame = self._spark.createDataFrame(arrow_table)
        writer = frame.write.mode("overwrite" if mode == "create" else "append")
        writer.saveAsTable(self._fqtn(table))
        return int(arrow_table.num_rows)

    def _describe_sync(self, table: TableRef) -> dict[str, str]:
        exists: Any = self._spark.catalog.tableExists(self._fqtn(table))
        if not exists:
            return {}
        schema = self._spark.table(self._fqtn(table)).schema
        return {field.name: spark_type_name(field.dataType.simpleString()) for field in schema.fields}
