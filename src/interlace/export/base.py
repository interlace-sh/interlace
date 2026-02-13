"""
Base exporter class and configuration.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from interlace.utils.logging import get_logger

logger = get_logger("interlace.export")


@dataclass
class ExportConfig:
    """Configuration for exporting a table to a file."""

    format: str = "csv"
    path: str = ""
    overwrite: bool = True

    # CSV-specific
    delimiter: str = ","
    header: bool = True
    quote: str = '"'
    escape: str = '"'
    null_string: str = ""

    # JSON-specific
    json_format: str = "records"  # "records" (array of objects) or "lines" (newline-delimited)
    orient: str = "records"  # pandas-style orient for compatibility

    # Parquet-specific
    compression: str = "snappy"  # snappy, gzip, zstd, none
    row_group_size: int | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ExportConfig:
        """Create ExportConfig from a dictionary, ignoring unknown keys."""
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}
        return cls(**filtered)


class Exporter(ABC):
    """Base class for file exporters."""

    @property
    @abstractmethod
    def format_name(self) -> str:
        """Return the format name (e.g. 'csv', 'parquet', 'json')."""
        ...

    @abstractmethod
    def export(
        self,
        connection: Any,
        table_name: str,
        schema: str,
        config: ExportConfig,
    ) -> str:
        """
        Export a table to a file.

        Args:
            connection: ibis backend connection
            table_name: Name of the table to export
            schema: Schema/database name
            config: Export configuration

        Returns:
            Path to the exported file
        """
        ...

    def _ensure_parent_dir(self, path: str) -> Path:
        """Ensure parent directory exists and return Path object."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def _qualify_table(self, table_name: str, schema: str) -> str:
        """Return schema-qualified table name."""
        return f"{schema}.{table_name}"

    def _execute_sql(self, connection: Any, sql: str) -> None:
        """Execute a SQL statement on the connection."""
        from interlace.core.context import _execute_sql_internal

        _execute_sql_internal(connection, sql)
