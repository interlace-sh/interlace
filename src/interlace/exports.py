"""Sinks / exports — pushing a model's result to an external destination.

A model with an ``export`` block is a *sink*: it produces no managed table and no
environment view; instead its (resolved) query result is written to a
destination. v1 supports file formats via DuckDB ``COPY``; database tables and
SaaS APIs will arrive through a ``SinkConnector`` (Arrow read -> push) with
upsert/append modes and a delivery ledger. Exports are side-effecting — no
view-swap rollback (see docs/architecture/v2-design.md §6).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp

from interlace.exceptions import ConfigurationError, PlanError

_FILE_FORMATS = frozenset({"parquet", "csv", "json"})


@dataclass(frozen=True)
class ExportConfig:
    """Where a sink writes. ``to`` is the destination type; ``path`` the target."""

    to: str
    path: str

    @classmethod
    def from_dict(cls, data: Any) -> ExportConfig:
        if not isinstance(data, dict) or "to" not in data or "path" not in data:
            raise ConfigurationError("export requires 'to' and 'path'", details={"got": data})
        return cls(to=str(data["to"]), path=str(data["path"]))


def export_statements(
    export: ExportConfig, query: exp.Expression, resolved_path: str, dialect: str
) -> list[exp.Expression]:
    """Build the statements that write ``query`` to the export destination."""
    if export.to not in _FILE_FORMATS:
        raise PlanError(f"unsupported export destination: {export.to!r}", details={"to": export.to})
    options = "FORMAT csv, HEADER" if export.to == "csv" else f"FORMAT {export.to}"
    query_sql = query.sql(dialect=dialect)
    escaped = resolved_path.replace("'", "''")
    return [sqlglot.parse_one(f"COPY ({query_sql}) TO '{escaped}' ({options})", read=dialect)]
