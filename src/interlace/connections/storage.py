"""
Storage connection base class.

Phase 0: Base class for storage connections (S3, local filesystem, etc.).
"""

from typing import Any


class BaseStorageConnection:
    """
    Base class for storage connections.

    Storage connections are for file/object storage operations (put/get files),
    not for SQL querying. For querying, use queryable connections with storage
    backends (e.g., DuckDB with S3 extension).

    Different storage backends have different interfaces:
    - S3: bucket + base_path
    - Local filesystem: root_path/base_path
    - Azure Blob: container + base_path
    - GCS: bucket + base_path

    Subclasses should implement storage-specific properties and methods.
    """

    def __init__(self, name: str, config: dict[str, Any]):
        """
        Initialize storage connection.

        Args:
            name: Connection name (from config)
            config: Connection configuration dictionary
        """
        self.name = name
        self.config = config

    @property
    def base_path(self) -> str:
        """
        Get base path for storage operations.

        For object storage (S3, GCS, Azure), this is the path within bucket/container.
        For local filesystem, this is the relative path from root.
        """
        storage = self.config.get("config", {}).get("storage", {})
        return storage.get("base_path", "")  # type: ignore[no-any-return]

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}(name='{self.name}')"
