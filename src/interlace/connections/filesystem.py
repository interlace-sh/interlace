"""
Local filesystem connection for storage.

Phase 0: Local filesystem connection for storage operations.
"""

from pathlib import Path
from typing import Any

from interlace.connections.storage import BaseStorageConnection


class FilesystemConnection(BaseStorageConnection):
    """
    Local filesystem connection wrapper for storage operations.

    Used for local file storage operations (put/get files).
    For querying local files via SQL, use DuckDB with file paths.
    """

    def __init__(self, name: str, config: dict[str, Any]):
        super().__init__(name, config)

    @property
    def root_path(self) -> Path:
        """
        Get root path for filesystem storage.

        Returns:
            Path: Root directory path for this storage connection
        """
        root = self.config.get("config", {}).get("root_path", "data")
        return Path(root)

    @property
    def full_path(self) -> Path:
        """
        Get full storage path (root_path + base_path).

        Returns:
            Path: Full path for storage operations

        Raises:
            ValueError: If base_path attempts path traversal outside root_path
        """
        base = self.base_path
        if base:
            # Resolve both paths to catch path traversal attempts
            root_resolved = self.root_path.resolve()
            full_resolved = (self.root_path / base).resolve()

            # Ensure full path is within or equal to root path
            try:
                full_resolved.relative_to(root_resolved)
            except ValueError as e:
                raise ValueError(
                    f"Path traversal detected: base_path '{base}' escapes root_path '{self.root_path}'"
                ) from e

            return full_resolved
        return self.root_path.resolve()

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}(name='{self.name}', root_path='{self.root_path}')"
