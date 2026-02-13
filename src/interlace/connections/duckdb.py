"""
DuckDB connection via ibis.

Phase 0: Basic DuckDB connection handling using ibis.
"""

from typing import Any

import ibis

from interlace.connections.base import BaseConnection
from interlace.core.context import _execute_sql_internal
from interlace.utils.logging import get_logger

logger = get_logger("interlace.connections.duckdb")


class DuckDBConnection(BaseConnection):
    """DuckDB connection wrapper using ibis."""

    def __init__(self, name: str, config: dict[str, Any]):
        super().__init__(name, config)

    @property
    def connection(self) -> ibis.BaseBackend:
        """
        Get DuckDB connection via ibis (lazy initialization).

        Returns:
            ibis.BaseBackend: ibis DuckDB backend
        """
        if self._connection is None:
            path = self.config.get("path", ":memory:")

            # Ensure directory exists for file-based databases
            if path != ":memory:" and not path.startswith("s3://"):
                from pathlib import Path

                db_path = Path(path)
                db_path.parent.mkdir(parents=True, exist_ok=True)

            # Connect via ibis DuckDB backend
            if path == ":memory:":
                self._connection = ibis.duckdb.connect()
            else:
                try:
                    self._connection = ibis.duckdb.connect(path)
                except Exception as e:
                    error_str = str(e)
                    # If connection fails due to file lock, raise clear error
                    if "lock" in error_str.lower() or "conflicting" in error_str.lower():
                        # Extract PID if available
                        import re

                        pid_match = re.search(r"PID\s+(\d+)", error_str)
                        pid_info = f" (PID: {pid_match.group(1)})" if pid_match else ""

                        # Raise a clean error without nesting - the traceback will show the chain
                        raise RuntimeError(
                            f"Cannot connect to DuckDB database '{path}': File is locked by another process{pid_info}.\n"
                            f"Please close any other processes accessing this database:\n"
                            f"  - DuckDB CLI sessions\n"
                            f"  - Other Python processes\n"
                            f"  - Database viewers or tools"
                        ) from e
                    # For other connection errors, provide context
                    raise RuntimeError(
                        f"Cannot connect to DuckDB database '{path}': {error_str}\n"
                        f"Please verify:\n"
                        f"  - Database file exists and is accessible\n"
                        f"  - File permissions are correct\n"
                        f"  - Database file is not corrupted"
                    ) from e
                # Enable WAL mode for better concurrent read performance
                # Note: WAL doesn't enable concurrent writes, but improves read performance
                # and durability. Multiple connections can read while one writes.
                try:
                    _execute_sql_internal(self._connection, "PRAGMA enable_checkpoint_on_shutdown")
                    # Note: DuckDB doesn't have explicit WAL mode like SQLite
                    # Instead, it uses a transaction log internally
                    # Multiple connections can read while one writes (MVCC)
                except (ibis.common.exceptions.IbisError, RuntimeError, AttributeError) as e:
                    # Expected errors: pragma not supported or connection issue
                    logger.debug(f"Pragma enable_checkpoint_on_shutdown not supported: {e}")
                except Exception as e:
                    # Unexpected error - log warning but continue
                    logger.warning(f"Unexpected error setting pragma on DuckDB connection: {e}")

            # Handle attach configuration
            # Attach external databases to DuckDB
            attach = self.config.get("attach", [])
            for attach_config in attach:
                self._attach_external(attach_config)

        return self._connection

    def _attach_external(self, attach_config: dict[str, Any]) -> None:
        """
        Attach external database to DuckDB.

        Supports attaching:
        - ``postgres`` -- PostgreSQL via DuckDB postgres extension
        - ``mysql`` -- MySQL via DuckDB mysql extension
        - ``sqlite`` -- SQLite via DuckDB sqlite extension
        - ``duckdb`` -- Another DuckDB file (for cross-file queries / shared source layer)
        - ``ducklake`` -- DuckLake lakehouse (time travel, S3-backed, SQL catalog)

        All types support ``read_only: true`` for safe cross-environment access.

        Uses DuckDB's ATTACH functionality via SQL execution.
        """
        attach_type = attach_config.get("type")
        name = attach_config.get("name", "main")
        read_only = attach_config.get("read_only", False)

        # Validate attach name to prevent SQL injection (alphanumeric + underscore only)
        if not name.replace("_", "").isalnum():
            raise ValueError(f"Invalid attach name '{name}': must be alphanumeric with underscores only")

        # Escape connection string values to prevent injection
        def escape_conn_value(val: str) -> str:
            return str(val).replace("'", "''").replace("\\", "\\\\")

        # Build READ_ONLY option clause
        def _options_clause(type_str: str = "", read_only: bool = False) -> str:
            """Build the options clause for ATTACH (TYPE ..., READ_ONLY)."""
            parts = []
            if type_str:
                parts.append(f"TYPE {type_str}")
            if read_only:
                parts.append("READ_ONLY")
            if parts:
                return f" ({', '.join(parts)})"
            return ""

        if attach_type == "postgres":
            db_config = attach_config.get("config", {})

            # Validate required fields
            required_fields = ["host", "database"]
            missing = [f for f in required_fields if not db_config.get(f)]
            if missing:
                raise ValueError(f"Postgres attach config missing required fields: {missing}")

            host = escape_conn_value(db_config.get("host", "localhost"))
            port = int(db_config.get("port", 5432))  # Force integer
            user = escape_conn_value(db_config.get("user", ""))
            password = escape_conn_value(db_config.get("password", ""))
            database = escape_conn_value(db_config.get("database", ""))

            conn_str = f"host={host} " f"port={port} " f"user={user} " f"password={password} " f"database={database}"
            options = _options_clause("POSTGRES", read_only)
            query = f"ATTACH '{conn_str}' AS {name}{options};"
            _execute_sql_internal(self._connection, query)
            logger.info(f"Attached Postgres database as '{name}'{' (read-only)' if read_only else ''}")

        elif attach_type == "mysql":
            db_config = attach_config.get("config", {})

            required_fields = ["host", "database"]
            missing = [f for f in required_fields if not db_config.get(f)]
            if missing:
                raise ValueError(f"MySQL attach config missing required fields: {missing}")

            host = escape_conn_value(db_config.get("host", "localhost"))
            port = int(db_config.get("port", 3306))
            user = escape_conn_value(db_config.get("user", ""))
            password = escape_conn_value(db_config.get("password", ""))
            database = escape_conn_value(db_config.get("database", ""))

            conn_str = f"host={host} " f"port={port} " f"user={user} " f"password={password} " f"database={database}"
            # Load mysql extension
            try:
                _execute_sql_internal(self._connection, "INSTALL mysql; LOAD mysql;")
            except Exception:
                logger.debug("MySQL extension may already be loaded")

            options = _options_clause("MYSQL", read_only)
            query = f"ATTACH '{conn_str}' AS {name}{options};"
            _execute_sql_internal(self._connection, query)
            logger.info(f"Attached MySQL database as '{name}'{' (read-only)' if read_only else ''}")

        elif attach_type == "sqlite":
            path = attach_config.get("path", "")
            if not path:
                raise ValueError("SQLite attach config missing required 'path'")

            escaped_path = escape_conn_value(path)

            # Load sqlite extension
            try:
                _execute_sql_internal(self._connection, "INSTALL sqlite; LOAD sqlite;")
            except Exception:
                logger.debug("SQLite extension may already be loaded")

            options = _options_clause("SQLITE", read_only)
            query = f"ATTACH '{escaped_path}' AS {name}{options};"
            _execute_sql_internal(self._connection, query)
            logger.info(f"Attached SQLite database as '{name}'{' (read-only)' if read_only else ''}")

        elif attach_type == "duckdb":
            # Cross-file DuckDB attach (e.g., shared source layer)
            path = attach_config.get("path", "")
            if not path:
                raise ValueError("DuckDB attach config missing required 'path'")

            escaped_path = escape_conn_value(path)
            options = _options_clause("", read_only)  # DuckDB is default type
            query = f"ATTACH '{escaped_path}' AS {name}{options};"
            _execute_sql_internal(self._connection, query)
            logger.info(f"Attached DuckDB database as '{name}'{' (read-only)' if read_only else ''}")

        elif attach_type == "ducklake":
            # DuckLake: SQL-based lakehouse format with time travel
            catalog = attach_config.get("catalog", "")
            data_path = attach_config.get("data_path", "")

            if not catalog:
                raise ValueError("DuckLake attach config missing required 'catalog'")

            escaped_catalog = escape_conn_value(catalog)

            # Load ducklake extension
            try:
                _execute_sql_internal(self._connection, "INSTALL ducklake; LOAD ducklake;")
            except Exception:
                logger.debug("DuckLake extension may already be loaded")

            # Build ATTACH string: ducklake:<catalog_connection_string>
            attach_str = f"ducklake:{escaped_catalog}"

            # Build options
            option_parts = []
            if read_only:
                option_parts.append("READ_ONLY")
            if data_path:
                escaped_data_path = escape_conn_value(data_path)
                option_parts.append(f"DATA_PATH '{escaped_data_path}'")

            options_str = f" ({', '.join(option_parts)})" if option_parts else ""
            query = f"ATTACH '{attach_str}' AS {name}{options_str};"
            _execute_sql_internal(self._connection, query)
            logger.info(f"Attached DuckLake as '{name}'{' (read-only)' if read_only else ''}")

        else:
            raise ValueError(
                f"Unsupported attach type '{attach_type}' for connection '{self.name}'. "
                f"Supported types: postgres, mysql, sqlite, duckdb, ducklake"
            )

    @staticmethod
    def build_attach_config_for_connection(
        conn_config: dict[str, Any],
        attach_name: str,
        read_only: bool = True,
    ) -> dict[str, Any]:
        """
        Build an attach config dict from a connection config.

        Useful for dynamically attaching one connection's database to another
        DuckDB connection (e.g., for fallback resolution or environment sharing).

        Args:
            conn_config: Source connection configuration
            attach_name: Name to give the attached database
            read_only: Whether to attach read-only

        Returns:
            Attach configuration dictionary
        """
        conn_type = conn_config.get("type", "duckdb")
        attach = {
            "type": conn_type,
            "name": attach_name,
            "read_only": read_only,
        }

        if conn_type == "duckdb":
            attach["path"] = conn_config.get("path", ":memory:")
        elif conn_type in ("postgres", "mysql"):
            attach["config"] = conn_config.get("config", {})
            # Also check top-level keys for postgres backward compat
            if conn_type == "postgres" and not attach["config"]:
                for key in ("host", "port", "user", "password", "database"):
                    if key in conn_config:
                        attach.setdefault("config", {})[key] = conn_config[key]
        elif conn_type == "sqlite":
            attach["path"] = conn_config.get("path", "")
        elif conn_type == "ducklake":
            attach["catalog"] = conn_config.get("catalog", "")
            attach["data_path"] = conn_config.get("data_path", "")

        return attach

    # execute() and close() are inherited from BaseConnection
