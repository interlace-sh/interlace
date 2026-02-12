"""
Tests for connection modules.

Tests for DuckDB, Postgres, and base connection classes.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock


class TestBaseConnection:
    """Tests for BaseConnection abstract class."""

    def test_base_connection_is_abstract(self):
        """Test that BaseConnection cannot be instantiated directly."""
        from interlace.connections.base import BaseConnection

        with pytest.raises(TypeError):
            BaseConnection("test", {})

    def test_base_connection_requires_connection_property(self):
        """Test that subclasses must implement connection property."""
        from interlace.connections.base import BaseConnection

        class IncompleteConnection(BaseConnection):
            pass

        with pytest.raises(TypeError):
            IncompleteConnection("test", {})


class TestDuckDBConnection:
    """Tests for DuckDBConnection."""

    def test_create_memory_connection(self):
        """Test creating an in-memory DuckDB connection."""
        from interlace.connections.duckdb import DuckDBConnection

        conn = DuckDBConnection("test", {"path": ":memory:"})

        # Connection is lazy - accessing .connection creates it
        backend = conn.connection
        assert backend is not None

        # Should be able to execute queries
        result = conn.execute("SELECT 1 as value")
        assert result.iloc[0]["value"] == 1

        conn.close()

    def test_create_file_connection(self):
        """Test creating a file-based DuckDB connection."""
        from interlace.connections.duckdb import DuckDBConnection

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            conn = DuckDBConnection("test", {"path": str(db_path)})

            backend = conn.connection
            assert backend is not None
            assert db_path.exists()

            conn.close()

    def test_connection_creates_parent_directories(self):
        """Test that connection creates parent directories if needed."""
        from interlace.connections.duckdb import DuckDBConnection

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "subdir" / "nested" / "test.duckdb"
            conn = DuckDBConnection("test", {"path": str(db_path)})

            backend = conn.connection
            assert db_path.parent.exists()

            conn.close()

    def test_connection_context_manager(self):
        """Test using connection as context manager."""
        from interlace.connections.duckdb import DuckDBConnection

        with DuckDBConnection("test", {"path": ":memory:"}) as conn:
            result = conn.execute("SELECT 42 as answer")
            assert result.iloc[0]["answer"] == 42

        # Connection should be closed after context
        assert conn._connection is None

    def test_connection_repr(self):
        """Test string representation."""
        from interlace.connections.duckdb import DuckDBConnection

        conn = DuckDBConnection("my_conn", {"path": ":memory:"})
        assert "DuckDBConnection" in repr(conn)
        assert "my_conn" in repr(conn)

    def test_lazy_initialization(self):
        """Test that connection is not created until accessed."""
        from interlace.connections.duckdb import DuckDBConnection

        conn = DuckDBConnection("test", {"path": ":memory:"})

        # Should not be connected yet
        assert conn._connection is None

        # Accessing connection property creates connection
        _ = conn.connection
        assert conn._connection is not None

        conn.close()

    def test_close_idempotent(self):
        """Test that closing multiple times is safe."""
        from interlace.connections.duckdb import DuckDBConnection

        conn = DuckDBConnection("test", {"path": ":memory:"})
        _ = conn.connection

        conn.close()
        conn.close()  # Should not raise
        assert conn._connection is None


class TestDuckDBConnectionAttach:
    """Tests for DuckDB ATTACH functionality."""

    def test_attach_config_parsed(self):
        """Test that attach config is parsed correctly."""
        from interlace.connections.duckdb import DuckDBConnection

        config = {
            "path": ":memory:",
            "attach": [
                {
                    "type": "postgres",
                    "name": "pg_db",
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "user": "test",
                        "password": "secret",
                        "database": "testdb",
                    },
                }
            ],
        }

        conn = DuckDBConnection("test", config)

        # Mock the SQL execution to verify ATTACH is called
        with patch.object(conn, "_attach_external") as mock_attach:
            _ = conn.connection
            # ATTACH should be called for each config
            mock_attach.assert_called_once()


class TestS3Connection:
    """Tests for S3Connection."""

    def test_s3_bucket_property(self):
        """Test that bucket property returns config value."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection("test", {"config": {"bucket": "my-bucket"}})
        assert conn.bucket == "my-bucket"

    def test_s3_region_property(self):
        """Test that region property returns config value."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection("test", {"config": {"bucket": "b", "region": "us-west-2"}})
        assert conn.region == "us-west-2"

    def test_s3_endpoint_url_property(self):
        """Test endpoint_url for S3-compatible services (MinIO, etc)."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection(
            "test",
            {"config": {"bucket": "b", "endpoint_url": "http://localhost:9000"}},
        )
        assert conn.endpoint_url == "http://localhost:9000"

    def test_s3_base_path_inherited(self):
        """Test that base_path is inherited from BaseStorageConnection."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection(
            "test", {"config": {"bucket": "my-bucket", "storage": {"base_path": "data/"}}}
        )
        assert conn.base_path == "data/"

    def test_s3_full_key_with_base_path(self):
        """Test that _full_key prepends base_path."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection(
            "test", {"config": {"bucket": "b", "storage": {"base_path": "prefix"}}}
        )
        assert conn._full_key("file.txt") == "prefix/file.txt"
        assert conn._full_key("/file.txt") == "prefix/file.txt"

    def test_s3_full_key_without_base_path(self):
        """Test that _full_key works without base_path."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection("test", {"config": {"bucket": "b"}})
        assert conn._full_key("file.txt") == "file.txt"
        assert conn._full_key("/file.txt") == "file.txt"

    def test_s3_client_lazy_initialization(self):
        """Test that client is lazily initialized."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection("test", {"config": {"bucket": "b"}})
        assert conn._client is None

        with patch("boto3.client") as mock_boto:
            mock_boto.return_value = MagicMock()
            _ = conn.client
            mock_boto.assert_called_once_with("s3")
            assert conn._client is not None

    def test_s3_client_with_credentials(self):
        """Test client initialization with explicit credentials."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection(
            "test",
            {
                "config": {
                    "bucket": "b",
                    "region": "eu-west-1",
                    "access_key_id": "AKIATEST",
                    "secret_access_key": "secret123",
                    "session_token": "token456",
                }
            },
        )

        with patch("boto3.client") as mock_boto:
            mock_boto.return_value = MagicMock()
            _ = conn.client
            mock_boto.assert_called_once_with(
                "s3",
                region_name="eu-west-1",
                aws_access_key_id="AKIATEST",
                aws_secret_access_key="secret123",
                aws_session_token="token456",
            )

    def test_s3_resource_lazy_initialization(self):
        """Test that resource is lazily initialized."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection("test", {"config": {"bucket": "b"}})
        assert conn._resource is None

        with patch("boto3.resource") as mock_boto:
            mock_boto.return_value = MagicMock()
            _ = conn.resource
            mock_boto.assert_called_once_with("s3")
            assert conn._resource is not None

    def test_s3_close_resets_clients(self):
        """Test that close() resets client and resource."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection("test", {"config": {"bucket": "b"}})
        conn._client = MagicMock()
        conn._resource = MagicMock()

        conn.close()

        assert conn._client is None
        assert conn._resource is None

    def test_s3_context_manager(self):
        """Test using S3Connection as context manager."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection("test", {"config": {"bucket": "b"}})
        conn._client = MagicMock()

        with conn as c:
            assert c is conn

        assert conn._client is None

    def test_s3_list_objects(self):
        """Test list_objects pagination."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection("test", {"config": {"bucket": "test-bucket"}})

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "file1.txt", "Size": 100}]},
            {"Contents": [{"Key": "file2.txt", "Size": 200}]},
        ]

        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        conn._client = mock_client

        objects = list(conn.list_objects(prefix="data/"))

        assert len(objects) == 2
        assert objects[0]["Key"] == "file1.txt"
        assert objects[1]["Key"] == "file2.txt"

    def test_s3_exists_true(self):
        """Test exists() returns True when object exists."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection("test", {"config": {"bucket": "b"}})
        mock_client = MagicMock()
        mock_client.head_object.return_value = {"ContentLength": 100}
        conn._client = mock_client

        assert conn.exists("file.txt") is True
        mock_client.head_object.assert_called_once_with(Bucket="b", Key="file.txt")

    def test_s3_exists_false(self):
        """Test exists() returns False when object doesn't exist."""
        from interlace.connections.s3 import S3Connection
        from botocore.exceptions import ClientError

        conn = S3Connection("test", {"config": {"bucket": "b"}})
        mock_client = MagicMock()

        # Simulate 404 error
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_client.head_object.side_effect = ClientError(error_response, "HeadObject")
        conn._client = mock_client

        assert conn.exists("nonexistent.txt") is False

    def test_s3_repr(self):
        """Test string representation."""
        from interlace.connections.s3 import S3Connection

        conn = S3Connection("my_s3", {"config": {"bucket": "b"}})
        assert "S3Connection" in repr(conn)
        assert "my_s3" in repr(conn)


class TestFilesystemConnection:
    """Tests for FilesystemConnection."""

    def test_filesystem_root_path(self):
        """Test root_path property."""
        from interlace.connections.filesystem import FilesystemConnection

        conn = FilesystemConnection("test", {"config": {"root_path": "/data/files"}})
        assert conn.root_path == Path("/data/files")

    def test_filesystem_full_path(self):
        """Test full_path combines root_path and base_path."""
        from interlace.connections.filesystem import FilesystemConnection

        conn = FilesystemConnection(
            "test",
            {"config": {"root_path": "/data", "storage": {"base_path": "incoming"}}},
        )
        assert conn.full_path == Path("/data/incoming")

    def test_filesystem_full_path_no_base(self):
        """Test full_path when no base_path is set."""
        from interlace.connections.filesystem import FilesystemConnection

        conn = FilesystemConnection("test", {"config": {"root_path": "/data"}})
        assert conn.full_path == Path("/data")


class TestSFTPConnection:
    """Tests for SFTPConnection."""

    def test_sftp_config_parsing(self):
        """Test that SFTP config is parsed correctly."""
        from interlace.connections.sftp import SFTPConnection, SFTPConfig

        conn = SFTPConnection(
            "test",
            {
                "config": {
                    "host": "sftp.example.com",
                    "port": 2222,
                    "username": "user",
                    "password": "pass123",
                    "connect_timeout_s": 30.0,
                }
            },
        )

        cfg = conn._parse_config()
        assert cfg.host == "sftp.example.com"
        assert cfg.port == 2222
        assert cfg.username == "user"
        assert cfg.password == "pass123"
        assert cfg.connect_timeout_s == 30.0

    def test_sftp_config_defaults(self):
        """Test that SFTP config defaults are applied."""
        from interlace.connections.sftp import SFTPConnection

        conn = SFTPConnection("test", {"config": {"host": "sftp.example.com"}})

        cfg = conn._parse_config()
        assert cfg.host == "sftp.example.com"
        assert cfg.port == 22  # Default
        assert cfg.username is None
        assert cfg.connect_timeout_s == 15.0  # Default

    def test_sftp_config_with_private_key(self):
        """Test SFTP config with private key authentication."""
        from interlace.connections.sftp import SFTPConnection

        conn = SFTPConnection(
            "test",
            {
                "config": {
                    "host": "sftp.example.com",
                    "username": "user",
                    "private_key_path": "/home/user/.ssh/id_rsa",
                    "private_key_passphrase": "keypass",
                }
            },
        )

        cfg = conn._parse_config()
        assert cfg.private_key_path == "/home/user/.ssh/id_rsa"
        assert cfg.private_key_passphrase == "keypass"

    def test_sftp_connect_requires_host(self):
        """Test that connect() raises if host is missing."""
        from interlace.connections.sftp import SFTPConnection

        conn = SFTPConnection("test", {"config": {}})

        with pytest.raises(ValueError) as exc_info:
            conn.connect()

        assert "missing host" in str(exc_info.value)

    def test_sftp_lazy_connection(self):
        """Test that connection is not created until connect() is called."""
        from interlace.connections.sftp import SFTPConnection

        conn = SFTPConnection("test", {"config": {"host": "example.com"}})

        assert conn._client is None
        assert conn._transport is None

    def test_sftp_close_idempotent(self):
        """Test that close() can be called multiple times safely."""
        from interlace.connections.sftp import SFTPConnection

        conn = SFTPConnection("test", {"config": {"host": "example.com"}})

        # Close without connecting - should not raise
        conn.close()
        conn.close()

        assert conn._client is None
        assert conn._transport is None

    def test_sftp_close_cleans_up(self):
        """Test that close() properly cleans up resources."""
        from interlace.connections.sftp import SFTPConnection

        conn = SFTPConnection("test", {"config": {"host": "example.com"}})

        # Mock connected state
        mock_client = MagicMock()
        mock_transport = MagicMock()
        conn._client = mock_client
        conn._transport = mock_transport

        conn.close()

        mock_client.close.assert_called_once()
        mock_transport.close.assert_called_once()
        assert conn._client is None
        assert conn._transport is None

    def test_sftp_context_manager(self):
        """Test using SFTPConnection as context manager."""
        from interlace.connections.sftp import SFTPConnection

        conn = SFTPConnection("test", {"config": {"host": "example.com"}})

        with patch.object(conn, "connect") as mock_connect:
            with patch.object(conn, "close") as mock_close:
                mock_connect.return_value = MagicMock()

                with conn as c:
                    assert c is conn
                    mock_connect.assert_called_once()

                mock_close.assert_called_once()

    def test_sftp_connect_with_password(self):
        """Test connection with password authentication."""
        from interlace.connections.sftp import SFTPConnection

        conn = SFTPConnection(
            "test",
            {
                "config": {
                    "host": "sftp.example.com",
                    "username": "user",
                    "password": "pass123",
                }
            },
        )

        with patch("paramiko.Transport") as mock_transport_cls:
            mock_transport = MagicMock()
            mock_transport_cls.return_value = mock_transport

            with patch("paramiko.SFTPClient.from_transport") as mock_sftp:
                mock_sftp.return_value = MagicMock()

                client = conn.connect()

                mock_transport_cls.assert_called_once_with(("sftp.example.com", 22))
                mock_transport.connect.assert_called_once_with(
                    username="user",
                    password="pass123",
                    pkey=None,
                )
                assert client is not None

    def test_sftp_connect_caches_client(self):
        """Test that repeated connect() returns cached client."""
        from interlace.connections.sftp import SFTPConnection

        conn = SFTPConnection("test", {"config": {"host": "example.com"}})

        mock_client = MagicMock()
        conn._client = mock_client

        # Should return cached client without creating new one
        result = conn.connect()
        assert result is mock_client


class TestConnectionManager:
    """Tests for ConnectionManager."""

    def test_get_connection_duckdb(self):
        """Test getting a DuckDB connection through manager."""
        from interlace.connections.manager import ConnectionManager

        config = {
            "connections": {
                "default": {"type": "duckdb", "config": {"path": ":memory:"}},
            }
        }

        manager = ConnectionManager(config, validate=False)
        conn = manager.get("default")

        assert conn is not None
        assert conn.name == "default"

        # Connection should be cached
        conn2 = manager.get("default")
        assert conn is conn2

    def test_get_connection_not_found(self):
        """Test getting a non-existent connection raises error."""
        from interlace.connections.manager import ConnectionManager

        config = {"connections": {}}
        manager = ConnectionManager(config, validate=False)

        with pytest.raises(Exception) as exc_info:
            manager.get("nonexistent")

        assert "nonexistent" in str(exc_info.value)

    def test_get_connection_unsupported_type(self):
        """Test unsupported connection type logs warning and skips."""
        from interlace.connections.manager import ConnectionManager

        config = {
            "connections": {
                "bad": {"type": "unsupported_db", "config": {}},
            }
        }

        # Unsupported types are skipped (logged warning), not raised
        manager = ConnectionManager(config, validate=False)

        # Connection should not exist since type was unsupported
        with pytest.raises(Exception) as exc_info:
            manager.get("bad")

        assert "bad" in str(exc_info.value)
