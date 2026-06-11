"""
Tests for the export module.

Tests CSV, JSON, and Parquet exporters using real DuckDB connections.
"""

import json
from pathlib import Path

import ibis
import pytest

from interlace.export.base import ExportConfig
from interlace.export.csv_exporter import CSVExporter
from interlace.export.json_exporter import JSONExporter
from interlace.export.parquet_exporter import ParquetExporter


@pytest.fixture
def duckdb_conn():
    """Create an in-memory DuckDB connection with a test table."""
    conn = ibis.duckdb.connect()
    conn.raw_sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    conn.raw_sql("""
        CREATE TABLE test_schema.users (
            id INTEGER,
            name VARCHAR,
            email VARCHAR
        )
        """)
    conn.raw_sql("""
        INSERT INTO test_schema.users VALUES
        (1, 'Alice', 'alice@example.com'),
        (2, 'Bob', 'bob@example.com'),
        (3, 'Charlie', 'charlie@example.com')
        """)
    yield conn
    conn.disconnect()


# -- ExportConfig tests --


class TestExportConfig:
    def test_defaults(self) -> None:
        config = ExportConfig()
        assert config.format == "csv"
        assert config.delimiter == ","
        assert config.header is True
        assert config.compression == "snappy"
        assert config.json_format == "records"
        assert config.overwrite is True

    def test_from_dict_overrides(self) -> None:
        config = ExportConfig.from_dict({"format": "json", "delimiter": "|", "header": False})
        assert config.format == "json"
        assert config.delimiter == "|"
        assert config.header is False

    def test_from_dict_ignores_unknown_keys(self) -> None:
        config = ExportConfig.from_dict({"format": "csv", "unknown_key": "ignored"})
        assert config.format == "csv"
        assert not hasattr(config, "unknown_key")

    def test_from_dict_empty(self) -> None:
        config = ExportConfig.from_dict({})
        assert config.format == "csv"
        assert config.delimiter == ","


# -- CSV Exporter tests --


class TestCSVExporter:
    def test_format_name(self) -> None:
        exporter = CSVExporter()
        assert exporter.format_name == "csv"

    def test_export_creates_file(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        exporter = CSVExporter()
        output = tmp_path / "users.csv"
        config = ExportConfig(path=str(output))

        result = exporter.export(duckdb_conn, "users", "test_schema", config)

        assert Path(result).exists()
        content = Path(result).read_text()
        assert "Alice" in content
        assert "Bob" in content
        assert "Charlie" in content

    def test_export_default_path(
        self, duckdb_conn: ibis.BaseBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        exporter = CSVExporter()
        config = ExportConfig()

        result = exporter.export(duckdb_conn, "users", "test_schema", config)

        assert result == "users.csv"

    def test_export_custom_delimiter(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        exporter = CSVExporter()
        output = tmp_path / "users_pipe.csv"
        config = ExportConfig(path=str(output), delimiter="|")

        exporter.export(duckdb_conn, "users", "test_schema", config)

        content = output.read_text()
        lines = content.strip().split("\n")
        assert "|" in lines[0]
        assert "," not in lines[0]

    def test_export_no_header(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        exporter = CSVExporter()
        output = tmp_path / "users_noheader.csv"
        config = ExportConfig(path=str(output), header=False)

        exporter.export(duckdb_conn, "users", "test_schema", config)

        content = output.read_text()
        lines = content.strip().split("\n")
        assert len(lines) == 3  # Data rows only, no header
        assert "id" not in lines[0]

    def test_export_creates_parent_dirs(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        exporter = CSVExporter()
        output = tmp_path / "nested" / "dir" / "users.csv"
        config = ExportConfig(path=str(output))

        exporter.export(duckdb_conn, "users", "test_schema", config)

        assert output.exists()


# -- JSON Exporter tests --


class TestJSONExporter:
    def test_format_name(self) -> None:
        exporter = JSONExporter()
        assert exporter.format_name == "json"

    def test_export_records_format(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        exporter = JSONExporter()
        output = tmp_path / "users.json"
        config = ExportConfig(path=str(output), json_format="records")

        result = exporter.export(duckdb_conn, "users", "test_schema", config)

        assert Path(result).exists()
        data = json.loads(Path(result).read_text())
        assert isinstance(data, list)
        assert len(data) == 3
        assert data[0]["name"] == "Alice"

    def test_export_ndjson_format(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        exporter = JSONExporter()
        output = tmp_path / "users.jsonl"
        config = ExportConfig(path=str(output), json_format="lines")

        result = exporter.export(duckdb_conn, "users", "test_schema", config)

        assert Path(result).exists()
        lines = Path(result).read_text().strip().split("\n")
        assert len(lines) == 3
        # Each line should be valid JSON
        for line in lines:
            obj = json.loads(line)
            assert "name" in obj

    def test_export_default_path_records(
        self, duckdb_conn: ibis.BaseBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        exporter = JSONExporter()
        config = ExportConfig(json_format="records")

        result = exporter.export(duckdb_conn, "users", "test_schema", config)
        assert result == "users.json"

    def test_export_default_path_ndjson(
        self, duckdb_conn: ibis.BaseBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        exporter = JSONExporter()
        config = ExportConfig(json_format="lines")

        result = exporter.export(duckdb_conn, "users", "test_schema", config)
        assert result == "users.jsonl"


# -- Parquet Exporter tests --


class TestParquetExporter:
    def test_format_name(self) -> None:
        exporter = ParquetExporter()
        assert exporter.format_name == "parquet"

    def test_export_creates_file(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        exporter = ParquetExporter()
        output = tmp_path / "users.parquet"
        config = ExportConfig(path=str(output))

        result = exporter.export(duckdb_conn, "users", "test_schema", config)

        assert Path(result).exists()
        assert Path(result).stat().st_size > 0

    def test_export_default_path(
        self, duckdb_conn: ibis.BaseBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        exporter = ParquetExporter()
        config = ExportConfig()

        result = exporter.export(duckdb_conn, "users", "test_schema", config)
        assert result == "users.parquet"

    def test_export_snappy_compression(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        exporter = ParquetExporter()
        output = tmp_path / "users_snappy.parquet"
        config = ExportConfig(path=str(output), compression="snappy")

        exporter.export(duckdb_conn, "users", "test_schema", config)
        assert output.exists()

    def test_export_no_compression(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        exporter = ParquetExporter()
        output = tmp_path / "users_none.parquet"
        config = ExportConfig(path=str(output), compression="none")

        exporter.export(duckdb_conn, "users", "test_schema", config)
        assert output.exists()

    def test_export_roundtrip(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        """Verify exported parquet can be read back with correct data."""
        exporter = ParquetExporter()
        output = tmp_path / "users_roundtrip.parquet"
        config = ExportConfig(path=str(output))

        exporter.export(duckdb_conn, "users", "test_schema", config)

        # Read back via DuckDB
        result = duckdb_conn.sql(f"SELECT * FROM read_parquet('{output}') ORDER BY id").execute()
        assert len(result) == 3
        assert list(result["name"]) == ["Alice", "Bob", "Charlie"]
