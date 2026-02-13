"""
Abstract base connection class for all ibis-based queryable connections.
"""

from abc import ABC, abstractmethod
from typing import Any

import ibis

from interlace.utils.logging import get_logger

logger = get_logger("interlace.connections.base")


class ReadOnlyConnectionError(RuntimeError):
    """Raised when a write operation is attempted on a read-only connection."""

    pass


class BaseConnection(ABC):
    """
    Base class for all queryable connections using ibis.

    Provides common interface and shared functionality for database connections
    that support SQL querying via ibis backends.

    Connection access policies:
        - ``access``: ``"readwrite"`` (default) or ``"read"`` -- controls whether
          write operations (create_table, raw DDL/DML) are permitted.
        - ``shared``: ``True`` or ``False`` (default) -- shared connections skip
          ``{env}`` substitution and are available across all environments.
    """

    def __init__(self, name: str, config: dict[str, Any]):
        """
        Initialize connection.

        Args:
            name: Connection name (from config)
            config: Connection configuration dictionary
        """
        self.name = name
        self.config = config
        self._connection: ibis.BaseBackend | None = None

        # Access policy: "read" or "readwrite" (default)
        self._access: str = config.get("access", "readwrite")
        if self._access not in ("read", "readwrite"):
            raise ValueError(
                f"Connection '{name}': invalid access policy '{self._access}'. " f"Must be 'read' or 'readwrite'."
            )

        # Shared flag: shared connections skip {env} substitution
        self._shared: bool = config.get("shared", False)

    @property
    def access(self) -> str:
        """Get the access policy for this connection ('read' or 'readwrite')."""
        return self._access

    @property
    def is_read_only(self) -> bool:
        """Whether this connection is read-only."""
        return self._access == "read"

    @property
    def is_shared(self) -> bool:
        """Whether this connection is shared across environments."""
        return self._shared

    def assert_writable(self, operation: str = "write") -> None:
        """
        Assert that this connection allows write operations.

        Args:
            operation: Description of the write operation (for error messages)

        Raises:
            ReadOnlyConnectionError: If connection is read-only
        """
        if self.is_read_only:
            raise ReadOnlyConnectionError(
                f"Cannot perform {operation} on read-only connection '{self.name}'. "
                f"This connection has access='read'. To allow writes, "
                f"set access='readwrite' in the connection config."
            )

    @property
    @abstractmethod
    def connection(self) -> ibis.BaseBackend:
        """
        Get ibis backend connection (lazy initialization).

        Subclasses must implement this to create and return the appropriate
        ibis backend connection.

        Returns:
            ibis.BaseBackend: ibis backend connection
        """
        pass

    def execute(self, query: str, **kwargs: Any) -> Any:
        """
        Execute SQL query via ibis backend.

        Common implementation for all ibis-based connections.

        Args:
            query: SQL query string
            **kwargs: Additional query parameters

        Returns:
            Query result
        """
        return self.connection.sql(query).execute(**kwargs)

    def close(self) -> None:
        """Close connection and cleanup resources."""
        if self._connection:
            # Try disconnect() first (preferred for ibis backends)
            if hasattr(self._connection, "disconnect"):
                try:
                    self._connection.disconnect()
                except Exception as e:
                    logger.debug(f"Error during disconnect() for {self.name}: {e}")

            # Also try close() if available (some backends need both)
            if hasattr(self._connection, "close"):
                try:
                    self._connection.close()
                except Exception as e:
                    logger.debug(f"Error during close() for {self.name}: {e}")

            self._connection = None

    def __enter__(self) -> "BaseConnection":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        """Context manager exit - closes connection."""
        try:
            self.close()
        except Exception as e:
            # Don't override the original exception if one occurred
            if exc_type is None:
                raise
            else:
                logger.warning(f"Error closing connection {self.name} during context exit: {e}")

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}(name='{self.name}')"
