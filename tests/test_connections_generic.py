"""
Tests for generic ibis connections, access policies, ATTACH extensions,
cache TTL, and fallback connection resolution.

Phase 3: Environment management and multi-backend support.
"""

import pytest
import time
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime, timezone
import ibis

from interlace.connections.base import BaseConnection, ReadOnlyConnectionError
from interlace.connections.ibis_generic import IbisConnection
from interlace.connections.duckdb import DuckDBConnection
from interlace.connections.manager import ConnectionManager
from interlace.core.model import parse_ttl


# ==============================================================================
# parse_ttl tests
# ==============================================================================


class TestParseTTL:
    """Tests for TTL string parsing."""

    def test_seconds(self):
        assert parse_ttl("30s") == 30.0

    def test_minutes(self):
        assert parse_ttl("5m") == 300.0

    def test_hours(self):
        assert parse_ttl("24h") == 86400.0

    def test_days(self):
        assert parse_ttl("7d") == 604800.0

    def test_weeks(self):
        assert parse_ttl("2w") == 1209600.0

    def test_float_value(self):
        assert parse_ttl("1.5h") == 5400.0

    def test_with_whitespace(self):
        assert parse_ttl("  7d  ") == 604800.0

    def test_case_insensitive(self):
        assert parse_ttl("7D") == 604800.0

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid TTL format"):
            parse_ttl("invalid")

    def test_invalid_unit(self):
        with pytest.raises(ValueError, match="Invalid TTL format"):
            parse_ttl("30x")

    def test_empty_string(self):
        with pytest.raises(ValueError, match="Invalid TTL format"):
            parse_ttl("")

    def test_no_number(self):
        with pytest.raises(ValueError, match="Invalid TTL format"):
            parse_ttl("d")


# ==============================================================================
# Access policy tests
# ==============================================================================


class TestAccessPolicies:
    """Tests for connection access policies (read/readwrite)."""

    def test_default_access_is_readwrite(self):
        """Default access policy should be readwrite."""
        conn = DuckDBConnection("test", {"type": "duckdb", "path": ":memory:"})
        assert conn.access == "readwrite"
        assert not conn.is_read_only

    def test_read_only_access(self):
        """Read-only connections should report as read-only."""
        conn = DuckDBConnection("test", {
            "type": "duckdb",
            "path": ":memory:",
            "access": "read",
        })
        assert conn.access == "read"
        assert conn.is_read_only

    def test_readwrite_access_explicit(self):
        """Explicit readwrite should work."""
        conn = DuckDBConnection("test", {
            "type": "duckdb",
            "path": ":memory:",
            "access": "readwrite",
        })
        assert conn.access == "readwrite"
        assert not conn.is_read_only

    def test_invalid_access_raises(self):
        """Invalid access policy should raise ValueError."""
        with pytest.raises(ValueError, match="invalid access policy"):
            DuckDBConnection("test", {
                "type": "duckdb",
                "path": ":memory:",
                "access": "invalid",
            })

    def test_assert_writable_passes_for_readwrite(self):
        """assert_writable should pass for readwrite connections."""
        conn = DuckDBConnection("test", {"type": "duckdb", "path": ":memory:"})
        conn.assert_writable("create table")  # Should not raise

    def test_assert_writable_raises_for_read_only(self):
        """assert_writable should raise for read-only connections."""
        conn = DuckDBConnection("test", {
            "type": "duckdb",
            "path": ":memory:",
            "access": "read",
        })
        with pytest.raises(ReadOnlyConnectionError, match="read-only connection"):
            conn.assert_writable("create table")

    def test_shared_flag_default_false(self):
        """Default shared flag should be False."""
        conn = DuckDBConnection("test", {"type": "duckdb", "path": ":memory:"})
        assert not conn.is_shared

    def test_shared_flag_true(self):
        """Shared flag should be True when configured."""
        conn = DuckDBConnection("test", {
            "type": "duckdb",
            "path": ":memory:",
            "shared": True,
        })
        assert conn.is_shared


# ==============================================================================
# IbisConnection tests
# ==============================================================================


class TestIbisConnection:
    """Tests for the generic IbisConnection class."""

    def test_supports_type_valid(self):
        """Should report support for known backend types."""
        assert IbisConnection.supports_type("mysql")
        assert IbisConnection.supports_type("snowflake")
        assert IbisConnection.supports_type("bigquery")
        assert IbisConnection.supports_type("clickhouse")
        assert IbisConnection.supports_type("sqlite")

    def test_supports_type_invalid(self):
        """Should not report support for unknown types."""
        assert not IbisConnection.supports_type("duckdb")  # Handled by specialised class
        assert not IbisConnection.supports_type("postgres")  # Handled by specialised class
        assert not IbisConnection.supports_type("unknown")

    def test_unsupported_type_raises(self):
        """Creating IbisConnection with unsupported type should raise."""
        with pytest.raises(ValueError, match="Unsupported ibis backend"):
            IbisConnection("test", {"type": "unknown"})

    def test_backend_type_property(self):
        """backend_type property should return the configured type."""
        conn = IbisConnection("test", {"type": "snowflake", "config": {}})
        assert conn.backend_type == "snowflake"

    def test_is_cloud_backend(self):
        """Cloud backends should be identified correctly."""
        conn = IbisConnection("test", {"type": "snowflake", "config": {}})
        assert conn.is_cloud_backend
        assert not conn.is_file_backend

    def test_is_file_backend(self):
        """File-based backends should be identified correctly."""
        conn = IbisConnection("test", {"type": "sqlite", "config": {}})
        assert conn.is_file_backend
        assert not conn.is_cloud_backend

    def test_neither_cloud_nor_file(self):
        """Some backends are neither cloud nor file."""
        conn = IbisConnection("test", {"type": "mysql", "config": {}})
        assert not conn.is_cloud_backend
        assert not conn.is_file_backend

    def test_build_connect_kwargs(self):
        """Should extract connect kwargs from config, removing interlace keys."""
        conn = IbisConnection("test", {
            "type": "snowflake",
            "config": {
                "account": "myorg",
                "user": "admin",
                "password": "secret",
                "storage": {"base_path": "data"},  # Should be removed
                "pool": {"max_size": 10},  # Should be removed
            },
        })
        kwargs = conn._build_connect_kwargs()
        assert kwargs == {"account": "myorg", "user": "admin", "password": "secret"}

    def test_missing_backend_import(self):
        """Should raise ImportError with helpful message when backend not installed."""
        conn = IbisConnection("test", {"type": "exasol", "config": {}})
        # Patch the import to simulate missing backend
        with patch("importlib.import_module", side_effect=ImportError("No module named 'ibis.exasol'")):
            with pytest.raises(ImportError, match="pip install"):
                _ = conn.connection

    def test_access_policies_on_ibis_connection(self):
        """IbisConnection should inherit access policies from BaseConnection."""
        conn = IbisConnection("test", {
            "type": "snowflake",
            "access": "read",
            "shared": True,
            "config": {},
        })
        assert conn.is_read_only
        assert conn.is_shared

    def test_backend_map_completeness(self):
        """Backend map should contain expected major backends."""
        expected = {
            "mysql", "sqlite", "snowflake", "bigquery", "clickhouse",
            "databricks", "trino", "mssql", "oracle", "datafusion",
        }
        assert expected.issubset(set(IbisConnection.BACKEND_MAP.keys()))


# ==============================================================================
# DuckDB ATTACH extension tests
# ==============================================================================


class TestDuckDBAttach:
    """Tests for DuckDB ATTACH extensions."""

    def test_attach_duckdb_cross_file(self, tmp_path):
        """Should attach another DuckDB file."""
        # Create source database
        source_path = str(tmp_path / "source.duckdb")
        source_conn = ibis.duckdb.connect(source_path)
        source_conn.raw_sql("CREATE SCHEMA IF NOT EXISTS main")
        source_conn.raw_sql("CREATE TABLE main.test_data (id INT, name VARCHAR)")
        source_conn.raw_sql("INSERT INTO main.test_data VALUES (1, 'hello')")
        source_conn.disconnect()

        # Create main database and attach source
        main_conn = DuckDBConnection("main", {
            "type": "duckdb",
            "path": str(tmp_path / "main.duckdb"),
            "attach": [{
                "type": "duckdb",
                "name": "source_db",
                "path": source_path,
                "read_only": True,
            }],
        })

        # Query across databases
        result = main_conn.connection.sql(
            "SELECT * FROM source_db.main.test_data"
        ).execute()
        assert len(result) == 1
        assert result.iloc[0]["name"] == "hello"

        main_conn.close()

    def test_attach_sqlite(self, tmp_path):
        """Should attach a SQLite database."""
        import sqlite3

        # Create SQLite database
        sqlite_path = str(tmp_path / "test.sqlite")
        sqlite_conn = sqlite3.connect(sqlite_path)
        sqlite_conn.execute("CREATE TABLE items (id INTEGER, value TEXT)")
        sqlite_conn.execute("INSERT INTO items VALUES (1, 'test')")
        sqlite_conn.commit()
        sqlite_conn.close()

        # Attach to DuckDB
        main_conn = DuckDBConnection("main", {
            "type": "duckdb",
            "path": ":memory:",
            "attach": [{
                "type": "sqlite",
                "name": "sqlite_db",
                "path": sqlite_path,
                "read_only": True,
            }],
        })

        result = main_conn.connection.sql(
            "SELECT * FROM sqlite_db.items"
        ).execute()
        assert len(result) == 1
        main_conn.close()

    def test_attach_invalid_type_raises(self):
        """Should raise for unsupported attach types."""
        with pytest.raises(ValueError, match="Unsupported attach type"):
            conn = DuckDBConnection("test", {
                "type": "duckdb",
                "path": ":memory:",
                "attach": [{
                    "type": "unknown_type",
                    "name": "test_db",
                }],
            })
            _ = conn.connection

    def test_attach_read_only_duckdb(self, tmp_path):
        """Read-only attached DuckDB should prevent writes."""
        # Create source database
        source_path = str(tmp_path / "source.duckdb")
        source_conn = ibis.duckdb.connect(source_path)
        source_conn.raw_sql("CREATE TABLE test (id INT)")
        source_conn.disconnect()

        # Attach as read-only
        main_conn = DuckDBConnection("main", {
            "type": "duckdb",
            "path": ":memory:",
            "attach": [{
                "type": "duckdb",
                "name": "ro_db",
                "path": source_path,
                "read_only": True,
            }],
        })

        # Writing should fail
        with pytest.raises(Exception):
            main_conn.connection.raw_sql("INSERT INTO ro_db.test VALUES (1)")

        main_conn.close()

    def test_attach_name_validation(self):
        """Should reject invalid attach names."""
        with pytest.raises(ValueError, match="Invalid attach name"):
            conn = DuckDBConnection("test", {
                "type": "duckdb",
                "path": ":memory:",
                "attach": [{
                    "type": "duckdb",
                    "name": "bad; DROP TABLE",
                    "path": "/tmp/test.duckdb",
                }],
            })
            _ = conn.connection

    def test_build_attach_config_for_duckdb(self):
        """Should build correct attach config from DuckDB connection config."""
        config = {"type": "duckdb", "path": "/data/sources.duckdb"}
        attach = DuckDBConnection.build_attach_config_for_connection(
            config, "sources", read_only=True
        )
        assert attach["type"] == "duckdb"
        assert attach["name"] == "sources"
        assert attach["read_only"] is True
        assert attach["path"] == "/data/sources.duckdb"

    def test_build_attach_config_for_postgres(self):
        """Should build correct attach config from Postgres connection config."""
        config = {
            "type": "postgres",
            "config": {"host": "localhost", "database": "mydb", "user": "admin"},
        }
        attach = DuckDBConnection.build_attach_config_for_connection(
            config, "pg_db", read_only=True
        )
        assert attach["type"] == "postgres"
        assert attach["name"] == "pg_db"
        assert attach["config"]["host"] == "localhost"


# ==============================================================================
# ConnectionManager tests
# ==============================================================================


class TestConnectionManager:
    """Tests for ConnectionManager with generic backend support."""

    def test_load_duckdb_connection(self):
        """Should load DuckDB connections."""
        config = {
            "connections": {
                "default": {"type": "duckdb", "path": ":memory:"},
            }
        }
        mgr = ConnectionManager(config, validate=False)
        conn = mgr.get("default")
        assert isinstance(conn, DuckDBConnection)

    def test_load_generic_ibis_connection(self):
        """Should load generic ibis connections for supported types."""
        config = {
            "connections": {
                "sf": {"type": "snowflake", "config": {"account": "test"}},
            }
        }
        mgr = ConnectionManager(config, validate=False)
        conn = mgr.get("sf")
        assert isinstance(conn, IbisConnection)
        assert conn.backend_type == "snowflake"

    def test_unknown_type_skipped(self):
        """Unknown connection types should be skipped with warning."""
        config = {
            "connections": {
                "unknown": {"type": "totally_unknown"},
            }
        }
        mgr = ConnectionManager(config, validate=False)
        assert "unknown" not in mgr.list()

    def test_get_shared_connections(self):
        """Should return only shared connections."""
        config = {
            "connections": {
                "default": {"type": "duckdb", "path": ":memory:"},
                "sources": {"type": "duckdb", "path": ":memory:", "shared": True},
            }
        }
        mgr = ConnectionManager(config, validate=False)
        shared = mgr.get_shared_connections()
        assert len(shared) == 1
        assert shared[0][0] == "sources"

    def test_get_fallback_connections_from_config(self):
        """Should return fallback connections from environments config."""
        config = {
            "connections": {
                "default": {"type": "duckdb", "path": ":memory:"},
                "sources": {"type": "duckdb", "path": ":memory:", "shared": True},
            },
            "environments": {
                "fallback_connections": ["sources"],
            },
        }
        mgr = ConnectionManager(config, validate=False)
        fallbacks = mgr.get_fallback_connections(config)
        assert len(fallbacks) == 1
        assert fallbacks[0][0] == "sources"

    def test_get_fallback_connections_defaults_to_shared(self):
        """Should use shared connections as fallback when no explicit config."""
        config = {
            "connections": {
                "default": {"type": "duckdb", "path": ":memory:"},
                "sources": {"type": "duckdb", "path": ":memory:", "shared": True},
            },
        }
        mgr = ConnectionManager(config, validate=False)
        fallbacks = mgr.get_fallback_connections(config)
        assert len(fallbacks) == 1
        assert fallbacks[0][0] == "sources"


# ==============================================================================
# Fallback resolution tests
# ==============================================================================


class TestFallbackResolution:
    """Tests for fallback connection dependency resolution."""

    async def test_dependency_loader_with_fallback(self):
        """DependencyLoader should find tables in fallback connections."""
        from interlace.core.execution.dependency_loader import DependencyLoader

        # Create source database with a table
        source_conn = ibis.duckdb.connect()
        source_conn.raw_sql("CREATE SCHEMA IF NOT EXISTS public")
        source_conn.raw_sql("CREATE TABLE public.source_data (id INT, name VARCHAR)")
        source_conn.raw_sql("INSERT INTO public.source_data VALUES (1, 'test')")

        # Create a mock connection object
        mock_source = MagicMock()
        mock_source.connection = source_conn

        # Create dependency loader with fallback
        loader = DependencyLoader(
            materialised_tables={},
            schema_manager=MagicMock(),
            dep_loading_locks={},
            dep_schema_cache={},
            fallback_connections=[("sources", mock_source)],
        )

        # The dependency should be found in fallback
        main_conn = ibis.duckdb.connect()  # Empty connection

        result = await loader.load_dependency_with_lock(
            "source_data", main_conn, {}, "public"
        )

        assert result is not None
        sources = loader.get_dependency_sources()
        assert sources.get("source_data") == "sources"

        source_conn.disconnect()
        main_conn.disconnect()

    async def test_dependency_loader_prefers_primary(self):
        """DependencyLoader should prefer primary connection over fallback."""
        from interlace.core.execution.dependency_loader import DependencyLoader

        # Create primary connection with a table
        primary_conn = ibis.duckdb.connect()
        primary_conn.raw_sql("CREATE SCHEMA IF NOT EXISTS public")
        primary_conn.raw_sql("CREATE TABLE public.my_table (id INT)")
        primary_conn.raw_sql("INSERT INTO public.my_table VALUES (1)")

        # Also create in fallback
        fallback_conn = ibis.duckdb.connect()
        fallback_conn.raw_sql("CREATE SCHEMA IF NOT EXISTS public")
        fallback_conn.raw_sql("CREATE TABLE public.my_table (id INT)")
        fallback_conn.raw_sql("INSERT INTO public.my_table VALUES (2)")

        mock_fallback = MagicMock()
        mock_fallback.connection = fallback_conn

        loader = DependencyLoader(
            materialised_tables={},
            schema_manager=MagicMock(),
            dep_loading_locks={},
            dep_schema_cache={},
            fallback_connections=[("fallback", mock_fallback)],
        )

        result = await loader.load_dependency_with_lock(
            "my_table", primary_conn, {}, "public"
        )

        assert result is not None
        sources = loader.get_dependency_sources()
        assert sources.get("my_table") == "primary"

        primary_conn.disconnect()
        fallback_conn.disconnect()


# ==============================================================================
# Cache TTL integration test
# ==============================================================================


class TestCacheTTLModelExecutor:
    """Tests for cache TTL in model executor context."""

    def test_cache_config_stored_in_model_metadata(self):
        """Cache config should be stored in model metadata."""
        from interlace.core.model import model

        @model(
            name="test_source",
            cache={"ttl": "7d", "strategy": "ttl"},
            tags=["source"],
        )
        def test_source():
            return [{"id": 1}]

        assert test_source._interlace_model["cache"] == {
            "ttl": "7d",
            "strategy": "ttl",
        }

    def test_cache_default_is_none(self):
        """Cache should default to None when not specified."""
        from interlace.core.model import model

        @model(name="no_cache")
        def no_cache():
            return [{"id": 1}]

        assert no_cache._interlace_model["cache"] is None
