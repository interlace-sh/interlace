"""
Connection management for creating per-task database connections.

Phase 0: Extracted from Executor class for better separation of concerns.
Phase 3: Extended for generic ibis backends and full ATTACH support.
"""

from typing import Any

import ibis

from interlace.utils.logging import get_logger

logger = get_logger("interlace.execution.connection_manager")


class TaskConnectionManager:
    """
    Manages per-task database connections.

    Creates isolated connections for each model execution task to enable
    safe parallel execution. Different strategies per backend type:

    - DuckDB: New connection per task (not thread-safe), except in-memory (shared)
    - Postgres: Connection pool if available, otherwise shared
    - Cloud backends (Snowflake, BigQuery): New connections (they pool internally)
    - File backends (SQLite, DataFusion): New connections per task for safety
    - Other backends: Reuse shared connection

    Note: This was renamed from ``ConnectionManager`` to avoid confusion with
    ``interlace.connections.manager.ConnectionManager`` which handles global
    connection loading and configuration.
    """

    def __init__(
        self,
        connection_configs: dict[str, dict[str, Any]],
        connections: dict[str, Any],
        default_conn_name: str,
        state_store: Any = None,
    ):
        """
        Initialize ConnectionManager.

        Args:
            connection_configs: Dictionary of connection configurations
            connections: Dictionary of shared connection instances
            default_conn_name: Name of default connection
            state_store: Optional StateStore (for DuckDB in-memory fallback)
        """
        self.connection_configs = connection_configs
        self._connection_configs = connection_configs  # Alias for backward compatibility
        self.connections = connections
        self.default_conn_name = default_conn_name
        self._default_conn_name = default_conn_name  # Alias for backward compatibility
        self.state_store = state_store
        # Get default connection for in-memory DuckDB sharing
        # Note: This will be set after connections are initialized
        self._default_connection = None

    def _get_default_connection(self):
        """Get the default connection, lazily initialised."""
        if self._default_connection is None:
            self._default_connection = self.connections.get(self.default_conn_name)
        return self._default_connection

    def _apply_attach_configs(self, new_conn, attach_configs):
        """
        Apply ATTACH configurations to a DuckDB connection.

        Delegates to DuckDBConnection._attach_external() pattern but works
        directly on an ibis backend connection for per-task connections.
        """
        from interlace.connections.duckdb import DuckDBConnection

        # Create a temporary wrapper to reuse the attach logic
        temp = DuckDBConnection.__new__(DuckDBConnection)
        temp.name = "task_connection"
        temp.config = {}
        temp._connection = new_conn
        temp._access = "readwrite"
        temp._shared = False

        for attach_config in attach_configs:
            temp._attach_external(attach_config)

    async def create_task_connection(self, conn_name: str) -> ibis.BaseBackend:
        """
        Create a new connection for the current task (async).

        DuckDB connections are NOT thread-safe, so we create separate connections per task
        to enable true parallel execution. DuckDB supports multiple connections to the same database file.

        For Postgres, uses connection pooling if enabled to limit concurrent connections.

        For cloud backends (Snowflake, BigQuery, etc.), creates new connections per task
        since they handle connection pooling internally.

        For generic ibis backends, checks if the connection object supports
        ``create_new_connection()`` for per-task creation.

        Args:
            conn_name: Connection name from config

        Returns:
            ibis.BaseBackend: New connection for this task
        """
        conn_config = self.connection_configs.get(conn_name)
        if conn_config is None:
            # Fallback to default
            conn_name = self.default_conn_name
            conn_config = self.connection_configs.get(conn_name, {})

        conn_type = conn_config.get("type", "duckdb")

        # For DuckDB: Create separate connections for parallel execution
        # DuckDB supports multiple connections to the same file-based database
        # For in-memory databases, we must share the single connection
        if conn_type == "duckdb":
            path = conn_config.get("path", ":memory:")

            # In-memory databases cannot be shared across connections
            # Must use the original shared connection
            if path == ":memory:":
                new_conn = self._get_default_connection()
                logger.debug("Using shared connection for in-memory DuckDB")
            else:
                # File-based database: create new connection to same file
                # This enables true parallel execution
                try:
                    new_conn = ibis.duckdb.connect(path)
                    logger.debug(f"Created new DuckDB connection to {path}")
                except Exception as e:
                    # If connection fails, fall back to shared connection
                    logger.warning(f"Failed to create new DuckDB connection: {e}. Using shared connection.")
                    new_conn = self._get_default_connection()

            # Apply attach configuration using the full ATTACH support
            attach = conn_config.get("attach", [])
            if attach:
                self._apply_attach_configs(new_conn, attach)

            return new_conn

        elif conn_type == "postgres":
            # For Postgres: Use connection pool if available
            conn_obj = self.connections.get(conn_name)
            if conn_obj and hasattr(conn_obj, "get_pooled_connection"):
                # Use async pool method
                return await conn_obj.get_pooled_connection()
            else:
                # Fallback to shared connection
                return self.connections.get(conn_name, self._get_default_connection())

        else:
            # For generic ibis backends: check the connection object
            conn_obj = self.connections.get(conn_name)

            if conn_obj and hasattr(conn_obj, "is_cloud_backend"):
                # IbisConnection with cloud backend awareness
                if conn_obj.is_cloud_backend:
                    # Cloud backends handle pooling internally -- create fresh connection
                    try:
                        new_conn = conn_obj.create_new_connection()
                        logger.debug(f"Created new {conn_type} connection for task")
                        return new_conn
                    except Exception as e:
                        logger.warning(f"Failed to create new {conn_type} connection: {e}. Using shared.")
                        return conn_obj.connection

                elif conn_obj.is_file_backend:
                    # File-based backends (SQLite, DataFusion) -- per-task for safety
                    try:
                        new_conn = conn_obj.create_new_connection()
                        logger.debug(f"Created new {conn_type} file-based connection for task")
                        return new_conn
                    except Exception as e:
                        logger.warning(f"Failed to create new {conn_type} connection: {e}. Using shared.")
                        return conn_obj.connection
                else:
                    # Other backends -- reuse shared connection
                    return conn_obj.connection

            elif conn_obj and hasattr(conn_obj, "connection"):
                # BaseConnection subclass -- reuse shared connection
                return conn_obj.connection

            else:
                # Fallback to default
                return self._get_default_connection()


# Backward-compatible alias (deprecated: use TaskConnectionManager instead)
ConnectionManager = TaskConnectionManager
