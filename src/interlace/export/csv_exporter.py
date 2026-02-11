"""
CSV exporter using DuckDB's native COPY TO.
"""

from __future__ import annotations

from typing import Any

from interlace.export.base import Exporter, ExportConfig, logger


class CSVExporter(Exporter):
    """Export tables to CSV files using DuckDB's COPY TO."""

    @property
    def format_name(self) -> str:
        return "csv"

    def export(
        self,
        connection: Any,
        table_name: str,
        schema: str,
        config: ExportConfig,
    ) -> str:
        """
        Export a table to a CSV file.

        Uses DuckDB's ``COPY ... TO ... (FORMAT CSV)`` for optimal performance.
        Falls back to pandas-based export for non-DuckDB backends.

        Args:
            connection: ibis backend connection
            table_name: Name of the table to export
            schema: Schema/database name
            config: Export configuration

        Returns:
            Path to the exported CSV file
        """
        if not config.path:
            config.path = f"{table_name}.csv"

        output_path = self._ensure_parent_dir(config.path)
        qualified = self._qualify_table(table_name, schema)

        # Build COPY options
        options = [
            "FORMAT CSV",
            f"HEADER {str(config.header).upper()}",
            f"DELIMITER '{config.delimiter}'",
        ]
        if config.quote != '"':
            options.append(f"QUOTE '{config.quote}'")
        if config.null_string:
            options.append(f"NULL '{config.null_string}'")

        options_str = ", ".join(options)
        sql = f"COPY {qualified} TO '{output_path}' ({options_str})"

        try:
            self._execute_sql(connection, sql)
            logger.info(f"Exported {qualified} to CSV: {output_path}")
        except Exception as e:
            # Fallback: use ibis/pandas
            logger.debug(f"DuckDB COPY failed ({e}), falling back to pandas export")
            try:
                table = connection.table(table_name, database=schema)
            except Exception:
                table = connection.table(f"{schema}.{table_name}")

            df = table.execute()
            df.to_csv(
                str(output_path),
                index=False,
                sep=config.delimiter,
                header=config.header,
                quotechar=config.quote,
                na_rep=config.null_string,
            )
            logger.info(f"Exported {qualified} to CSV (pandas fallback): {output_path}")

        return str(output_path)
