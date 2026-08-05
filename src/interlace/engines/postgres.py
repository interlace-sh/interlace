"""Postgres engine adapter — the first native remote engine (ADBC transport).

Strategies execute *inside* Postgres: canonical ASTs transpile to the postgres
dialect and run over one ADBC connection; results come back as Arrow and bulk
loads go in via ``adbc_ingest``. Shared ADBC machinery lives in
:class:`~interlace.engines.adbc.AdbcAdapter`; this adds the driver, the
capability flags, and an ``information_schema`` probe for ``describe``.

Capability honesty drives the strategy fallbacks: Postgres has no
``CREATE OR REPLACE TABLE`` (Replace falls back to DROP+CREATE) and no
star-EXCLUDE projection (scd enumerates the model's columns instead of
``SELECT * EXCLUDE``). ``merge`` upserts with a native ``MERGE``; ``full_merge``
is portable by construction (set difference).

Requires the ``adbc`` extra (``pip install 'interlaced[adbc]'``).
"""

from __future__ import annotations

from interlace.engines.adbc import AdbcAdapter
from interlace.engines.base import EngineCaps
from interlace.exceptions import ConfigurationError
from interlace.ir.relation import TableRef

_POSTGRES_CAPS = EngineCaps(
    supports_create_or_replace=False,  # no CREATE OR REPLACE TABLE -> DROP+CREATE fallback
    supports_star_exclude=False,  # no SELECT * EXCLUDE -> scd enumerates the model's columns instead
    supports_merge=True,  # MERGE ... (PostgreSQL >= 15)
)


class PostgresAdapter(AdbcAdapter):
    """Executes canonical ASTs inside Postgres; Arrow in and out via ADBC."""

    dialect = "postgres"
    caps = _POSTGRES_CAPS

    @classmethod
    def connect(cls, dsn: str) -> PostgresAdapter:
        try:
            import adbc_driver_postgresql.dbapi as dbapi  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - import guard
            raise ConfigurationError(
                "the postgres engine needs the 'adbc' extra: pip install 'interlaced[adbc]'"
            ) from exc
        return cls(dbapi.connect(dsn))

    # information_schema is exact and cheap on Postgres — prefer it to the generic probe.
    async def table_exists(self, table: TableRef) -> bool:
        import asyncio

        return await asyncio.to_thread(self._table_exists_sync, table)

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
