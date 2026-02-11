"""
JSON exporter using DuckDB's native COPY TO.
"""

from __future__ import annotations

import json as json_module
from typing import Any

from interlace.export.base import Exporter, ExportConfig, logger


class JSONExporter(Exporter):
    """Export tables to JSON files using DuckDB's COPY TO."""

    @property
    def format_name(self) -> str:
        return "json"

    def export(
        self,
        connection: Any,
        table_name: str,
        schema: str,
        config: ExportConfig,
    ) -> str:
        """
        Export a table to a JSON file.

        Supports two formats:
        - ``records``: Array of JSON objects (default)
        - ``lines``: Newline-delimited JSON (one JSON object per line, NDJSON)

        Uses DuckDB's ``COPY ... TO ... (FORMAT JSON)`` when available.
        Falls back to pandas-based export for non-DuckDB backends.

        Args:
            connection: ibis backend connection
            table_name: Name of the table to export
            schema: Schema/database name
            config: Export configuration

        Returns:
            Path to the exported JSON file
        """
        if not config.path:
            suffix = ".jsonl" if config.json_format == "lines" else ".json"
            config.path = f"{table_name}{suffix}"

        output_path = self._ensure_parent_dir(config.path)
        qualified = self._qualify_table(table_name, schema)

        # DuckDB COPY TO JSON produces newline-delimited JSON by default
        # For "records" format, we'll use pandas fallback for proper JSON array output
        if config.json_format == "lines":
            # NDJSON via DuckDB COPY
            sql = f"COPY {qualified} TO '{output_path}' (FORMAT JSON)"
            try:
                self._execute_sql(connection, sql)
                logger.info(f"Exported {qualified} to NDJSON: {output_path}")
                return str(output_path)
            except Exception as e:
                logger.debug(f"DuckDB COPY JSON failed ({e}), falling back to pandas")

        # Fallback / records format: use pandas
        try:
            table = connection.table(table_name, database=schema)
        except Exception:
            table = connection.table(f"{schema}.{table_name}")

        df = table.execute()

        if config.json_format == "lines":
            # NDJSON format
            df.to_json(str(output_path), orient="records", lines=True, date_format="iso")
        else:
            # Records format (JSON array)
            records = df.to_dict(orient="records")
            with open(str(output_path), "w") as f:
                json_module.dump(records, f, indent=2, default=str)

        logger.info(f"Exported {qualified} to JSON ({config.json_format}): {output_path}")
        return str(output_path)
