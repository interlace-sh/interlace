"""
Sync subsystem (ingestion-stage jobs).

Phase A: SFTP polling + sync to S3/filesystem, with pluggable file processors.
"""

from interlace.sync.sftp_sync import run_sftp_sync_job
from interlace.sync.types import SFTPSyncJob, ProcessorSpec

__all__ = [
    "SFTPSyncJob",
    "ProcessorSpec",
    "run_sftp_sync_job",
]

