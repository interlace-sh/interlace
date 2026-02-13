"""
Export module for writing model output to files.

Supports CSV, Parquet, and JSON formats using DuckDB's native
COPY TO functionality for optimal performance.
"""

from interlace.export.base import ExportConfig, Exporter
from interlace.export.csv_exporter import CSVExporter
from interlace.export.json_exporter import JSONExporter
from interlace.export.parquet_exporter import ParquetExporter

__all__ = [
    "Exporter",
    "ExportConfig",
    "CSVExporter",
    "ParquetExporter",
    "JSONExporter",
    "get_exporter",
    "export_table",
]


def get_exporter(format: str) -> Exporter:
    """
    Get an exporter instance for the given format.

    Args:
        format: Export format ("csv", "parquet", "json")

    Returns:
        Exporter instance

    Raises:
        ValueError: If format is not supported
    """
    exporters = {
        "csv": CSVExporter,
        "parquet": ParquetExporter,
        "json": JSONExporter,
    }
    cls = exporters.get(format.lower())
    if cls is None:
        supported = ", ".join(sorted(exporters.keys()))
        raise ValueError(f"Unsupported export format: '{format}'. Supported formats: {supported}")
    return cls()


def export_table(
    connection,
    table_name: str,
    schema: str,
    export_config: dict,
) -> str:
    """
    Export a materialised table to a file.

    This is the main entry point for the export system. Called after
    materialization to write output to the specified format.

    Args:
        connection: ibis backend connection
        table_name: Name of the table to export
        schema: Schema/database name
        export_config: Export configuration dict with keys:
            - format: "csv", "parquet", or "json"
            - path: Output file path
            - Additional format-specific options

    Returns:
        Path to the exported file
    """
    fmt = export_config.get("format", "csv")
    exporter = get_exporter(fmt)
    config = ExportConfig.from_dict(export_config)
    return exporter.export(connection, table_name, schema, config)
