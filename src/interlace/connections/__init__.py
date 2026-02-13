"""
Connection management.

Phase 0: Connection loading and management (DuckDB, Postgres, S3, Filesystem).
Phase 3: Generic ibis backend support, access policies, shared connections.
"""

from interlace.connections.base import BaseConnection, ReadOnlyConnectionError
from interlace.connections.duckdb import DuckDBConnection
from interlace.connections.filesystem import FilesystemConnection
from interlace.connections.ibis_generic import IbisConnection
from interlace.connections.manager import ConnectionManager, get_connection, init_connections
from interlace.connections.postgres import PostgresConnection
from interlace.connections.s3 import S3Connection
from interlace.connections.sftp import SFTPConnection
from interlace.connections.storage import BaseStorageConnection

__all__ = [
    "BaseConnection",
    "ReadOnlyConnectionError",
    "BaseStorageConnection",
    "ConnectionManager",
    "get_connection",
    "init_connections",
    "DuckDBConnection",
    "PostgresConnection",
    "IbisConnection",
    "S3Connection",
    "FilesystemConnection",
    "SFTPConnection",
]
