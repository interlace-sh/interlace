"""
S3 connection for storage operations.

Provides boto3 client for S3 operations (list, download, upload).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterator, Optional

from interlace.connections.storage import BaseStorageConnection


class S3Connection(BaseStorageConnection):
    """
    S3 connection wrapper for storage operations.

    Provides lazy-initialized boto3 client with credential management.
    Supports AWS credentials from config, environment, or IAM role.

    Config example:
        connections:
          s3_data:
            type: s3
            config:
              bucket: my-bucket
              region: us-east-1
              access_key_id: AKIA...  # Optional, uses env/IAM if not set
              secret_access_key: ...   # Optional
              session_token: ...       # Optional (for temp creds)
              endpoint_url: ...        # Optional (for S3-compatible services)
              storage:
                base_path: data/raw    # Optional prefix for all operations
    """

    def __init__(self, name: str, config: dict[str, Any]):
        super().__init__(name, config)
        self._client = None
        self._resource = None
        # Validate bucket is configured
        bucket_name = self._cfg.get("bucket", "")
        if not bucket_name:
            raise ValueError(
                f"S3 connection '{name}' requires 'bucket' in config. "
                f"Example: connections.{name}.config.bucket = 'my-bucket'"
            )

    @property
    def bucket(self) -> str:
        """Get S3 bucket name from config."""
        bucket_name = self._cfg.get("bucket", "")
        if not bucket_name:
            raise ValueError(
                f"S3 connection '{self._name}' missing required 'bucket' configuration"
            )
        return bucket_name

    @property
    def region(self) -> Optional[str]:
        """Get AWS region from config."""
        return self._cfg.get("region")

    @property
    def endpoint_url(self) -> Optional[str]:
        """Get custom endpoint URL (for S3-compatible services like MinIO)."""
        return self._cfg.get("endpoint_url")

    @property
    def _cfg(self) -> dict[str, Any]:
        """Get nested config dict."""
        return self.config.get("config", {})

    def _get_client_kwargs(self) -> dict[str, Any]:
        """Build kwargs for boto3 client/resource initialization."""
        kwargs: dict[str, Any] = {}

        if self.region:
            kwargs["region_name"] = self.region

        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url

        # Explicit credentials from config (override env/IAM)
        access_key = self._cfg.get("access_key_id")
        secret_key = self._cfg.get("secret_access_key")
        session_token = self._cfg.get("session_token")

        if access_key and secret_key:
            kwargs["aws_access_key_id"] = access_key
            kwargs["aws_secret_access_key"] = secret_key
            if session_token:
                kwargs["aws_session_token"] = session_token

        return kwargs

    @property
    def client(self):
        """
        Get boto3 S3 client (lazy initialization).

        Returns:
            boto3.client('s3') instance
        """
        if self._client is None:
            import boto3

            self._client = boto3.client("s3", **self._get_client_kwargs())
        return self._client

    @property
    def resource(self):
        """
        Get boto3 S3 resource (lazy initialization).

        The resource provides higher-level object-oriented interface.

        Returns:
            boto3.resource('s3') instance
        """
        if self._resource is None:
            import boto3

            self._resource = boto3.resource("s3", **self._get_client_kwargs())
        return self._resource

    def _full_key(self, key: str) -> str:
        """Prepend base_path to key if configured."""
        if self.base_path:
            return f"{self.base_path.strip('/')}/{key.lstrip('/')}"
        return key.lstrip("/")

    def list_objects(
        self,
        prefix: str = "",
        *,
        max_keys: int = 1000,
        delimiter: str = "",
    ) -> Iterator[dict[str, Any]]:
        """
        List objects in the bucket with optional prefix.

        Args:
            prefix: Key prefix to filter objects (combined with base_path)
            max_keys: Maximum number of keys to return per request
            delimiter: Delimiter for grouping (e.g., '/' for directory-like listing)

        Yields:
            Dict with object metadata (Key, Size, LastModified, ETag, etc.)
        """
        full_prefix = self._full_key(prefix) if prefix else (self.base_path or "")
        paginator = self.client.get_paginator("list_objects_v2")

        page_config = {"MaxKeys": max_keys}
        if delimiter:
            page_config["Delimiter"] = delimiter

        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix, **page_config):
            for obj in page.get("Contents", []):
                yield obj

    def download_file(self, key: str, local_path: str | Path) -> Path:
        """
        Download an object to a local file.

        Args:
            key: S3 object key (base_path is prepended automatically)
            local_path: Local file path to save to

        Returns:
            Path to the downloaded file
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        full_key = self._full_key(key)

        # Download to temp file first for atomicity
        tmp_path = local_path.with_suffix(local_path.suffix + ".part")
        try:
            self.client.download_file(self.bucket, full_key, str(tmp_path))
            os.replace(tmp_path, local_path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        return local_path

    def upload_file(
        self,
        local_path: str | Path,
        key: str,
        *,
        extra_args: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Upload a local file to S3.

        Args:
            local_path: Local file path to upload
            key: S3 object key (base_path is prepended automatically)
            extra_args: Extra arguments for upload (ContentType, Metadata, etc.)

        Returns:
            S3 URI of uploaded object (s3://bucket/key)
        """
        full_key = self._full_key(key)

        # Upload atomically via temp key
        tmp_key = f"{full_key}.tmp"
        try:
            self.client.upload_file(
                str(local_path),
                self.bucket,
                tmp_key,
                ExtraArgs=extra_args,
            )
            # Copy to final location
            self.client.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": tmp_key},
                Key=full_key,
            )
            # Delete temp
            self.client.delete_object(Bucket=self.bucket, Key=tmp_key)
        except Exception:
            # Cleanup temp on failure
            try:
                self.client.delete_object(Bucket=self.bucket, Key=tmp_key)
            except Exception:
                pass
            raise

        return f"s3://{self.bucket}/{full_key}"

    def delete_object(self, key: str) -> None:
        """
        Delete an object from S3.

        Args:
            key: S3 object key (base_path is prepended automatically)
        """
        full_key = self._full_key(key)
        self.client.delete_object(Bucket=self.bucket, Key=full_key)

    def get_object(self, key: str) -> bytes:
        """
        Get object content as bytes.

        Args:
            key: S3 object key (base_path is prepended automatically)

        Returns:
            Object content as bytes
        """
        full_key = self._full_key(key)
        response = self.client.get_object(Bucket=self.bucket, Key=full_key)
        return response["Body"].read()

    def put_object(
        self,
        key: str,
        body: bytes | str,
        *,
        content_type: Optional[str] = None,
    ) -> str:
        """
        Put object content directly (for small objects).

        Args:
            key: S3 object key (base_path is prepended automatically)
            body: Object content (bytes or string)
            content_type: Optional content type header

        Returns:
            S3 URI of uploaded object
        """
        full_key = self._full_key(key)

        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": full_key,
            "Body": body.encode() if isinstance(body, str) else body,
        }
        if content_type:
            kwargs["ContentType"] = content_type

        self.client.put_object(**kwargs)
        return f"s3://{self.bucket}/{full_key}"

    def exists(self, key: str) -> bool:
        """
        Check if an object exists.

        Args:
            key: S3 object key (base_path is prepended automatically)

        Returns:
            True if object exists, False otherwise
        """
        full_key = self._full_key(key)
        try:
            self.client.head_object(Bucket=self.bucket, Key=full_key)
            return True
        except Exception as e:
            # Handle ClientError for 404 / NoSuchKey / NotFound
            error_code = None
            http_status = None
            if hasattr(e, "response"):
                error_code = e.response.get("Error", {}).get("Code")
                http_status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            # Check for "not found" conditions
            not_found_codes = ("404", "NoSuchKey", "NotFound")
            if error_code in not_found_codes:
                return False
            # Check HTTP status code (e.g., 404)
            if http_status == 404:
                return False
            # Also check exception class name for NoSuchKey
            if type(e).__name__ in ("NoSuchKey", "ClientError"):
                # For ClientError, check if it's a 404
                if "Not Found" in str(e) or "404" in str(e):
                    return False
            raise

    def close(self) -> None:
        """Close S3 client connections."""
        # boto3 clients don't require explicit closing,
        # but we reset for consistency
        self._client = None
        self._resource = None

    def __enter__(self) -> "S3Connection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
