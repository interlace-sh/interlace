"""BigQuery engine adapter (ALPHA — not exercised against a live project).

An ADBC-transport engine built on :class:`~interlace.engines.adbc.AdbcAdapter`
(execute / fetch → Arrow / ``adbc_ingest`` load / metadata ``describe``); only the
dialect and capabilities differ. BigQuery supports the full strategy set:
``CREATE OR REPLACE TABLE``, ``SELECT * EXCEPT`` (sqlglot renders star-exclude in
the BigQuery dialect, so scd works with ``SELECT *``), and a native ``MERGE``. A
"schema" is a BigQuery dataset — ``CREATE SCHEMA IF NOT EXISTS`` creates one.

Requires the ``adbc-bigquery`` extra. Auth/project/dataset are driver ``db_kwargs``
(ADC or a service-account JSON); the exact keys need validating against a live
project — refine ``connect`` then.
"""

from __future__ import annotations

from interlace.engines.adbc import AdbcAdapter
from interlace.engines.base import EngineCaps
from interlace.exceptions import ConfigurationError

_BIGQUERY_CAPS = EngineCaps(
    supports_create_or_replace=True,
    supports_star_exclude=True,  # SELECT * EXCEPT (...)
    supports_merge=True,
)


class BigQueryAdapter(AdbcAdapter):
    """Executes canonical ASTs inside BigQuery; Arrow in and out via ADBC."""

    dialect = "bigquery"
    caps = _BIGQUERY_CAPS

    @classmethod
    def connect(cls, dsn: str) -> BigQueryAdapter:
        try:
            import adbc_driver_bigquery.dbapi as dbapi
        except ImportError as exc:  # pragma: no cover - import guard
            raise ConfigurationError(
                "the bigquery engine needs the 'adbc-bigquery' extra: pip install 'interlaced[adbc-bigquery]'"
            ) from exc
        # The BigQuery driver is configured through db_kwargs; the project id is the
        # one required key. Additional auth/dataset kwargs are added here once a live
        # project confirms the exact names.
        return cls(dbapi.connect(db_kwargs={"adbc.bigquery.sql.project_id": dsn}))
