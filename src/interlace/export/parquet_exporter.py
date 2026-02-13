"""
Parquet exporter using DuckDB's native COPY TO.
"""

from __future__ import annotations

from typing import Any

from interlace.export.base import ExportConfig, Exporter, logger


class ParquetExporter(Exporter):
    """Export tables to Parquet files using DuckDB's COPY TO."""

    @property
    def format_name(self) -> str:
        return "parquet"

    def export(
        self,
        connection: Any,
        table_name: str,
        schema: str,
        config: ExportConfig,
    ) -> str:
        """
        Export a table to a Parquet file.

        Uses DuckDB's ``COPY ... TO ... (FORMAT PARQUET)`` for optimal performance.
        Falls back to pandas/pyarrow-based export for non-DuckDB backends.

        Args:
            connection: ibis backend connection
            table_name: Name of the table to export
            schema: Schema/database name
            config: Export configuration

        Returns:
            Path to the exported Parquet file
        """
        if not config.path:
            config.path = f"{table_name}.parquet"

        output_path = self._ensure_parent_dir(config.path)
        qualified = self._qualify_table(table_name, schema)

        # Build COPY options
        options = ["FORMAT PARQUET"]
        if config.compression and config.compression != "none":
            options.append(f"CODEC '{config.compression.upper()}'")
        if config.row_group_size:
            options.append(f"ROW_GROUP_SIZE {config.row_group_size}")

        options_str = ", ".join(options)
        sql = f"COPY {qualified} TO '{output_path}' ({options_str})"

        try:
            self._execute_sql(connection, sql)
            logger.info(f"Exported {qualified} to Parquet: {output_path}")
        except Exception as e:
            # Fallback: use ibis/pandas + pyarrow
            logger.debug(f"DuckDB COPY failed ({e}), falling back to pandas export")
            try:
                table = connection.table(table_name, database=schema)
            except Exception:
                table = connection.table(f"{schema}.{table_name}")

            df = table.execute()
            df.to_parquet(
                str(output_path),
                compression=config.compression if config.compression != "none" else None,
                index=False,
            )
            logger.info(f"Exported {qualified} to Parquet (pandas fallback): {output_path}")

        return str(output_path)
