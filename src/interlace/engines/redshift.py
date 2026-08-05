"""Redshift engine adapter (ALPHA — not exercised against a live cluster).

Redshift speaks the Postgres wire protocol, so it reuses the Postgres ADBC driver
and its ``information_schema`` probes verbatim — only the SQL dialect and the
capability flags differ. Like Postgres it has no ``CREATE OR REPLACE TABLE`` and
no ``SELECT * EXCLUDE`` (scd enumerates the model's columns), but it does have a
native ``MERGE`` (Redshift, late 2023).

Requires the ``adbc`` extra. Connect string is a standard Postgres DSN pointed at
the Redshift endpoint (``postgresql://user:pass@cluster.<...>.redshift.amazonaws.com:5439/db``).
"""

from __future__ import annotations

from interlace.engines.base import EngineCaps
from interlace.engines.postgres import PostgresAdapter

_REDSHIFT_CAPS = EngineCaps(
    supports_create_or_replace=False,  # DROP+CREATE fallback
    supports_star_exclude=False,  # scd enumerates the model's columns instead
    supports_merge=True,  # MERGE (Redshift, 2023)
)


class RedshiftAdapter(PostgresAdapter):
    """Postgres-wire adapter with the Redshift dialect and capabilities."""

    dialect = "redshift"
    caps = _REDSHIFT_CAPS
