"""
StateStore-backed manifest for sync jobs.

Used to avoid re-downloading/re-processing unchanged SFTP files.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from interlace.core.context import _execute_sql_internal
from interlace.utils.logging import get_logger

logger = get_logger("interlace.sync.manifest")


@dataclass(frozen=True)
class ManifestKey:
    job_id: str
    remote_path: str
    remote_mtime: int
    remote_size: int


class SyncManifest:
    """Manifest accessors."""

    def __init__(self, state_connection: Any):
        self.state_connection = state_connection

    def has_processed(self, key: ManifestKey) -> bool:
        """
        Check if a remote file version has already been processed successfully.
        """
        try:
            query = f"""
                SELECT 1
                FROM interlace.file_sync_manifest
                WHERE job_id = '{_escape(key.job_id)}'
                  AND remote_path = '{_escape(key.remote_path)}'
                  AND remote_mtime = {int(key.remote_mtime)}
                  AND remote_size = {int(key.remote_size)}
                  AND status = 'success'
                LIMIT 1
            """
            result = _execute_sql_internal(self.state_connection, query)
            return _result_has_rows(result)
        except Exception as e:
            logger.debug(f"Manifest lookup failed (treat as not processed): {e}")
            return False

    def record_success(
        self,
        *,
        key: ManifestKey,
        dest_uri: str,
        remote_fingerprint: str | None = None,
    ) -> None:
        self._record(
            key=key,
            dest_uri=dest_uri,
            remote_fingerprint=remote_fingerprint,
            status="success",
            error_message=None,
        )

    def record_error(
        self,
        *,
        key: ManifestKey,
        dest_uri: str | None = None,
        remote_fingerprint: str | None = None,
        error_message: str,
    ) -> None:
        self._record(
            key=key,
            dest_uri=dest_uri or "",
            remote_fingerprint=remote_fingerprint,
            status="error",
            error_message=error_message,
        )

    def _record(
        self,
        *,
        key: ManifestKey,
        dest_uri: str,
        remote_fingerprint: str | None,
        status: str,
        error_message: str | None,
    ) -> None:
        """
        Record a manifest row. DuckDB UPSERT support varies, so use delete+insert.
        """
        try:
            delete_q = f"""
                DELETE FROM interlace.file_sync_manifest
                WHERE job_id = '{_escape(key.job_id)}'
                  AND remote_path = '{_escape(key.remote_path)}'
                  AND remote_mtime = {int(key.remote_mtime)}
                  AND remote_size = {int(key.remote_size)}
            """
            _execute_sql_internal(self.state_connection, delete_q)
        except Exception:
            pass

        fp_value = f"'{_escape(remote_fingerprint)}'" if remote_fingerprint else "NULL"
        err_value = f"'{_escape(error_message)}'" if error_message else "NULL"
        insert_q = f"""
            INSERT INTO interlace.file_sync_manifest
            (job_id, remote_path, remote_mtime, remote_size, remote_fingerprint, dest_uri, status, error_message)
            VALUES (
                '{_escape(key.job_id)}',
                '{_escape(key.remote_path)}',
                {int(key.remote_mtime)},
                {int(key.remote_size)},
                {fp_value},
                '{_escape(dest_uri)}',
                '{_escape(status)}',
                {err_value}
            )
        """
        _execute_sql_internal(self.state_connection, insert_q)


def _escape(value: str) -> str:
    return value.replace("'", "''")


def _result_has_rows(result: Any) -> bool:
    """
    Best-effort check for non-empty result across backends.
    """
    if result is None:
        return False
    if hasattr(result, "__len__"):
        try:
            return len(result) > 0
        except Exception:
            return False
    return False
