"""Snowflake engine adapter (ALPHA — not exercised against a live account).

An ADBC-transport engine: it inherits all of :class:`~interlace.engines.adbc.AdbcAdapter`
(execute / fetch → Arrow / ``adbc_ingest`` bulk load / metadata ``describe``) and
only sets the dialect and capabilities. Snowflake supports the full strategy set:
``CREATE OR REPLACE TABLE``, ``SELECT * EXCLUDE`` (so scd works with ``SELECT *``),
and a native ``MERGE``.

Requires the ``adbc-snowflake`` extra. The connection string is the Snowflake ADBC
URI (``user[:password]@account/database/schema?warehouse=WH&role=R``); key-pair and
external-browser auth go through ``db_kwargs`` — refine here once validated live.
"""

from __future__ import annotations

from interlace.engines.adbc import AdbcAdapter
from interlace.engines.base import EngineCaps
from interlace.exceptions import ConfigurationError

_SNOWFLAKE_CAPS = EngineCaps(
    supports_create_or_replace=True,
    supports_star_exclude=True,  # SELECT * EXCLUDE (...)
    supports_merge=True,
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
