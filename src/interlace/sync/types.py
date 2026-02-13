"""
Type definitions for sync jobs and file processors.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class ProcessorSpec:
    """Config-derived processor spec (name + config)."""

    name: str
    config: dict[str, Any] | None = None


class FileProcessor(Protocol):
    """
    File processor protocol.

    Processors operate on local files produced by sync (downloaded from SFTP),
    and must produce an output local file path (may be same file for no-op).
    """

    name: str

    def process(self, input_path: str, *, metadata: dict[str, Any]) -> tuple[str, dict[str, Any]]: ...


@dataclass(frozen=True)
class SFTPSyncJob:
    """
    Config for a single SFTP sync job.

    Phase A: polling + diff via mtime/size, optional future hash fingerprinting.
    """

    job_id: str
    sftp_connection: str
    remote_glob: str
    destination_connection: str
    destination_prefix: str = ""
    processors: list[ProcessorSpec] | None = None
    # Concurrency/backpressure
    max_files_per_run: int = 200
    # If true, only size+mtime diffing; if false, may later support remote hash/fingerprint
    change_detection: str = "mtime_size"
    # Local working directory (temp staging)
    work_dir: str = "/tmp/interlace-sync"


@dataclass(frozen=True)
class RemoteFile:
    path: str
    mtime: int
    size: int
    filename: str


@dataclass(frozen=True)
class DestURI:
    uri: str
    connection: str
    key_or_path: str
    bucket: str | None = None
