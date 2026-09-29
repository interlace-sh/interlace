"""Snowflake engine adapter (ALPHA — exercised against one account, not in CI).

An ADBC-transport engine: it inherits all of :class:`~interlace.engines.adbc.AdbcAdapter`
(execute / fetch → Arrow / ``adbc_ingest`` bulk load / metadata ``describe``) and
only sets the dialect and capabilities. Snowflake supports the full strategy set:
``CREATE OR REPLACE TABLE``, ``SELECT * EXCLUDE`` (so scd works with ``SELECT *``),
and a native ``MERGE``.

Snowflake folds unquoted identifiers to uppercase and returns those names. Fetch
and describe fold them back, so contracts and Python models keep the names the
SQL was written with. ``adbc_ingest`` on this driver rejects a target schema, so
loads ``USE SCHEMA`` first.

Requires the ``adbc-snowflake`` extra. The connection string is the Snowflake ADBC
URI (``user[:password]@account/database/schema?warehouse=WH&role=R``); key-pair and
external-browser auth go through ``db_kwargs``.
"""

from __future__ import annotations

import pyarrow as pa
from sqlglot import exp

from interlace.engines.adbc import AdbcAdapter
from interlace.engines.base import EngineCaps, LoadMode
from interlace.exceptions import ConfigurationError
from interlace.ir.relation import TableRef

_SNOWFLAKE_CAPS = EngineCaps(
    supports_create_or_replace=True,
    supports_star_exclude=True,  # SELECT * EXCLUDE (...)
    supports_merge=True,
    supports_transactions=True,
    # PRIMARY KEY / UNIQUE / FOREIGN KEY are informational. NOT NULL is enforced.
    enforced_constraints=frozenset({"not_null"}),
)


class SnowflakeAdapter(AdbcAdapter):
    """Executes canonical ASTs inside Snowflake; Arrow in and out via ADBC."""

    dialect = "snowflake"
    caps = _SNOWFLAKE_CAPS

    @classmethod
    def connect(cls, dsn: str) -> SnowflakeAdapter:
        try:
            import adbc_driver_snowflake.dbapi as dbapi
        except ImportError as exc:  # pragma: no cover - import guard
            raise ConfigurationError(
                "the snowflake engine needs the 'adbc-snowflake' extra: pip install 'interlaced[adbc-snowflake]'"
            ) from exc
        return cls(dbapi.connect(dsn))

    def _fetch_sync(self, sql: str) -> pa.RecordBatchReader:
        table = super()._fetch_sync(sql).read_all()
        return table.rename_columns([name.casefold() for name in table.column_names]).to_reader()

    def _describe_sync(self, table: TableRef) -> dict[str, str]:
        # The driver quotes the name it is given. Unquoted SQL folds to uppercase,
        # so a lowercase lookup looks for a different table and comes back empty.
        folded = TableRef(
            schema=table.schema.upper(),
            name=table.name.upper(),
            catalog=table.catalog.upper() if table.catalog else None,
        )
        described = super()._describe_sync(folded)
        return {name.casefold(): kind for name, kind in described.items()}

    def _load_sync(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> int:
        # This driver rejects adbc.ingest.target_db_schema. The session schema is
        # the ingest target; unquoted names fold the same way the model's SQL does.
        ingest_mode = "replace" if mode == "create" else "append"
        schema = exp.to_identifier(table.schema).sql(dialect=self.dialect) if table.schema else ""
        # Arrow names are the SQL as written (lowercase). The driver quotes them,
        # so they must be the uppercase form unquoted SQL folds to.
        loaded_table = reader.read_all()
        batch = loaded_table.rename_columns([name.upper() for name in loaded_table.column_names])
        with self._lock:
            try:
                with self._conn.cursor() as cur:
                    if schema:
                        cur.execute(f"USE SCHEMA {schema}")
                    loaded = cur.adbc_ingest(table.name.upper(), batch, mode=ingest_mode)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return int(loaded) if isinstance(loaded, int) and loaded > 0 else 0
