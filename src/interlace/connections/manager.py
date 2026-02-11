"""
Connection manager.

Phase 0: Manage connections to various data sources.
Phase 3: Generic ibis backend support, shared connections, access policies.
"""

import threading
from typing import Dict, Any, List, Optional
from interlace.connections.duckdb import DuckDBConnection
from interlace.connections.postgres import PostgresConnection
from interlace.connections.ibis_generic import IbisConnection
from interlace.connections.s3 import S3Connection
from interlace.connections.filesystem import FilesystemConnection
from interlace.connections.sftp import SFTPConnection
from interlace.utils.logging import get_logger

logger = get_logger("interlace.connections.manager")


class ConnectionManager:
    """
    Manages connections to data sources.

    Supports:
    - DuckDB and Postgres as specialised backends
    - Any ibis-supported backend via IbisConnection (Snowflake, BigQuery, MySQL, etc.)
    - Storage connections (S3, Filesystem, SFTP)
    - Shared connections (``shared: true``) that span environments
    - Access policies (``access: read | readwrite``)
    """

    def __init__(self, config: Dict[str, Any], validate: bool = True):
        self.config = config
        self._connections: Dict[str, Any] = {}
        self._load_connections()
        
        if validate:
            self.validate()

    def _load_connections(self):
        """Load connections from configuration."""
        connections_config = self.config.get("connections", {})

        for name, conn_config in connections_config.items():
            conn_type = conn_config.get("type")

            # Specialised backends with unique behaviour
            if conn_type == "duckdb":
                self._connections[name] = DuckDBConnection(name, conn_config)
            elif conn_type == "postgres":
                self._connections[name] = PostgresConnection(name, conn_config)

            # Storage connections
            elif conn_type == "s3":
                self._connections[name] = S3Connection(name, conn_config)
            elif conn_type == "filesystem":
                self._connections[name] = FilesystemConnection(name, conn_config)
            elif conn_type == "sftp":
                self._connections[name] = SFTPConnection(name, conn_config)

            # Generic ibis backends (Snowflake, BigQuery, MySQL, ClickHouse, etc.)
            elif IbisConnection.supports_type(conn_type):
                self._connections[name] = IbisConnection(name, conn_config)
                logger.info(f"Registered {conn_type} connection '{name}' via generic ibis backend")

            # Not-yet-implemented types
            elif conn_type in ("http", "api"):
                logger.warning(
                    f"Connection type '{conn_type}' not yet implemented, skipping connection '{name}'"
                )
                continue
            else:
                logger.warning(
                    f"Unknown connection type '{conn_type}' for connection '{name}', skipping"
                )
                continue

    def get(self, name: str) -> Any:
        """Get connection by name."""
        if name not in self._connections:
            raise ValueError(f"Connection not found: {name}")
        return self._connections[name]

    def list(self) -> list[str]:
        """List all connection names."""
        return list(self._connections.keys())

    def get_shared_connections(self) -> List[tuple]:
        """
        Get all shared connections as (name, connection_object) tuples.

        Shared connections (``shared: true``) are available across all
        environments and can be used as fallback connections for
        virtual environment dependency resolution.

        Returns:
            List of (name, connection_object) tuples
        """
        shared = []
        for name, conn_obj in self._connections.items():
            if hasattr(conn_obj, "is_shared") and conn_obj.is_shared:
                shared.append((name, conn_obj))
        return shared

    def get_fallback_connections(self, config: Dict[str, Any]) -> List[tuple]:
        """
        Get fallback connections for dependency resolution.

        Reads ``environments.fallback_connections`` from config and returns
        the matching connection objects. Falls back to shared connections
        if no explicit fallback list is configured.

        Args:
            config: Project configuration dictionary

        Returns:
            List of (name, connection_object) tuples
        """
        env_config = config.get("environments", {})
        fallback_names = env_config.get("fallback_connections", [])

        if fallback_names:
            fallbacks = []
            for name in fallback_names:
                if name in self._connections:
                    fallbacks.append((name, self._connections[name]))
                else:
                    logger.warning(f"Fallback connection '{name}' not found in connections config")
            return fallbacks

        # Default: use all shared connections as fallbacks
        return self.get_shared_connections()
    
    def validate(self) -> None:
        """Validate all connections can be accessed."""
        connections_config = self.config.get("connections", {})
        errors = []
        
        # Check if any connections are defined
        if not connections_config:
            # No connections defined - this is a warning, not an error
            import logging
            logger = logging.getLogger("interlace.connections")
            logger.warning("No connections defined in configuration")
            return
        
        # Try to connect to each connection
        for conn_name in self._connections.keys():
            try:
                conn_obj = self._connections[conn_name]
                # Try to access the connection (this will trigger actual connection)
                try:
                    _ = conn_obj.connection
                except Exception as e:
                    error_msg = str(e)
                    # Format helpful error messages
                    if "lock" in error_msg.lower():
                        errors.append(
                            f"Connection '{conn_name}': Cannot connect - database file is locked.\n"
                            f"  Error: {error_msg}\n"
                            f"  Suggestion: Close other processes accessing the database"
                        )
                    elif "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                        errors.append(
                            f"Connection '{conn_name}': Cannot connect - file or resource not found.\n"
                            f"  Error: {error_msg}\n"
                            f"  Suggestion: Check that the database file/path exists and is accessible"
                        )
                    elif "permission denied" in error_msg.lower():
                        errors.append(
                            f"Connection '{conn_name}': Cannot connect - permission denied.\n"
                            f"  Error: {error_msg}\n"
                            f"  Suggestion: Check file/directory permissions"
                        )
                    else:
                        errors.append(
                            f"Connection '{conn_name}': Cannot connect.\n"
                            f"  Error: {error_msg}"
                        )
            except Exception as e:
                errors.append(
                    f"Connection '{conn_name}': Unexpected error during connection test:\n"
                    f"  Error: {e}"
                )
        
        if errors:
            error_message = "Connection validation failed:\n\n" + "\n\n".join(f"{i}. {err}" for i, err in enumerate(errors, 1))
            raise ValueError(error_message)


# Global connection manager instance with thread-safe access
_connection_manager: Optional[ConnectionManager] = None
_connection_manager_lock = threading.Lock()


def get_connection(name: str) -> Any:
    """
    Get connection by name (thread-safe).

    Phase 0: Get connection from global connection manager.
    """
    with _connection_manager_lock:
        if _connection_manager is None:
            raise RuntimeError("Connection manager not initialized. Call init_connections() first.")
        return _connection_manager.get(name)


def init_connections(config: Dict[str, Any], validate: bool = True):
    """Initialize global connection manager with validation (thread-safe)."""
    global _connection_manager
    with _connection_manager_lock:
        _connection_manager = ConnectionManager(config, validate=validate)
