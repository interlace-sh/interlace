"""
SFTP -> destination sync job runner.

Phase A: Poll SFTP, detect changed files, download to local staging, run processors,
then upload to destination (S3 or filesystem). Persists a manifest row per processed
remote file version in StateStore.
"""

from __future__ import annotations

import fnmatch
import os
import shutil
from pathlib import Path
from typing import Any

from interlace.connections.manager import get_connection
from interlace.core.state import StateStore
from interlace.processors.registry import build_default_processor_registry
from interlace.sync.manifest import ManifestKey, SyncManifest
from interlace.sync.types import DestURI, FileProcessor, RemoteFile, SFTPSyncJob
from interlace.utils.logging import get_logger

logger = get_logger("interlace.sync.sftp")


def run_sftp_sync_job(
    job: SFTPSyncJob,
    *,
    config: dict[str, Any],
    state_store: StateStore | None,
    processor_registry: dict[str, FileProcessor] | None = None,
) -> dict[str, Any]:
    """
    Run a single SFTP sync job once (one polling tick).

    Returns a summary dict for logs/observability.
    """
    processor_registry = processor_registry or build_default_processor_registry()
    state_conn = None
    manifest = None
    if state_store:
        try:
            state_conn = state_store._get_connection()
        except Exception:
            state_conn = None
    if state_conn is not None:
        manifest = SyncManifest(state_conn)

    sftp_conn = get_connection(job.sftp_connection)
    dest_conn = get_connection(job.destination_connection)

    work_dir = Path(job.work_dir) / job.job_id
    work_dir.mkdir(parents=True, exist_ok=True)

    processed = 0
    skipped = 0
    errors = 0
    outputs: list[str] = []

    # Resolve which remote files to consider
    with sftp_conn:
        client = sftp_conn.connect()
        remote_files = _list_matching_remote_files(client, job.remote_glob)

        for rf in remote_files[: job.max_files_per_run]:
            key = ManifestKey(job_id=job.job_id, remote_path=rf.path, remote_mtime=rf.mtime, remote_size=rf.size)
            if manifest and manifest.has_processed(key):
                skipped += 1
                continue

            local_in = work_dir / "in" / _safe_relpath(rf.path)
            local_in.parent.mkdir(parents=True, exist_ok=True)
            local_out = work_dir / "out" / _safe_relpath(rf.path)
            local_out.parent.mkdir(parents=True, exist_ok=True)

            try:
                # Download
                _download_sftp_file(client, rf.path, str(local_in))

                # Process pipeline (no-op if none configured)
                current_path = str(local_in)
                meta: dict[str, Any] = {
                    "job_id": job.job_id,
                    "remote_path": rf.path,
                    "remote_mtime": rf.mtime,
                    "remote_size": rf.size,
                }
                for spec in job.processors or []:
                    proc = processor_registry.get(spec.name)
                    if proc is None:
                        raise ValueError(
                            f"Unknown processor '{spec.name}' for job '{job.job_id}'. "
                            f"Available: {list(processor_registry.keys())}"
                        )
                    # Attach processor config into metadata for this processor invocation
                    meta = {**meta, **({"processor_config": spec.config} if spec.config else {})}
                    current_path, meta = proc.process(current_path, metadata=meta)

                # If processor chain produced a different file path, copy it into out staging
                if current_path != str(local_in):
                    shutil.copy2(current_path, str(local_out))
                    final_local = str(local_out)
                else:
                    # If unchanged, stage original into out path for upload
                    shutil.copy2(str(local_in), str(local_out))
                    final_local = str(local_out)

                dest = _resolve_destination(dest_conn, job.destination_prefix, rf.path)
                _upload_to_destination(dest_conn, dest, final_local)

                if manifest:
                    manifest.record_success(key=key, dest_uri=dest.uri)
                processed += 1
                outputs.append(dest.uri)
            except Exception as e:
                errors += 1
                if manifest:
                    manifest.record_error(key=key, dest_uri="", error_message=str(e))
                logger.error(f"SFTP sync error job={job.job_id} file={rf.path}: {e}")

    return {
        "job_id": job.job_id,
        "processed": processed,
        "skipped": skipped,
        "errors": errors,
        "outputs": outputs[:25],
    }


def _list_matching_remote_files(client: Any, remote_glob: str) -> list[RemoteFile]:
    """
    List remote files matching a glob like: /path/to/dir/*.csv or /path/**/*.parquet.

    Phase A: supports `**` by walking directories.
    """
    remote_glob = remote_glob.strip()
    if not remote_glob.startswith("/"):
        # be explicit: SFTP paths should be absolute to avoid surprises
        raise ValueError(f"remote_glob must be absolute, got: {remote_glob}")

    # Split into base directory and pattern
    base_dir, pattern = _split_glob(remote_glob)
    results: list[RemoteFile] = []

    def walk(dir_path: str) -> None:
        for attr in client.listdir_attr(dir_path):
            name = attr.filename
            full_path = f"{dir_path.rstrip('/')}/{name}"
            # paramiko SFTPAttributes: st_mode encodes file type bits
            is_dir = bool(getattr(attr, "st_mode", 0) & 0o040000)
            if is_dir:
                # Only walk when pattern includes **
                if "**" in remote_glob:
                    walk(full_path)
                continue

            if fnmatch.fnmatch(full_path, remote_glob):
                results.append(
                    RemoteFile(
                        path=full_path,
                        filename=name,
                        mtime=int(getattr(attr, "st_mtime", 0) or 0),
                        size=int(getattr(attr, "st_size", 0) or 0),
                    )
                )

    walk(base_dir)
    # Deterministic ordering: newest first (often what you want for polling)
    results.sort(key=lambda r: (r.mtime, r.path), reverse=True)
    return results


def _split_glob(remote_glob: str) -> tuple[str, str]:
    # Find the first glob token
    tokens = ["**", "*", "?", "["]
    idxs = [remote_glob.find(t) for t in tokens if remote_glob.find(t) != -1]
    if not idxs:
        # Exact file path case
        base = str(Path(remote_glob).parent)
        return base, remote_glob
    first = min(idxs)
    # Walk from first glob back to previous slash
    slash = remote_glob.rfind("/", 0, first)
    base_dir = remote_glob[:slash] if slash > 0 else "/"
    return base_dir, remote_glob


def _download_sftp_file(client: Any, remote_path: str, local_path: str) -> None:
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    tmp_path = f"{local_path}.part"
    client.get(remote_path, tmp_path)
    os.replace(tmp_path, local_path)


def _safe_relpath(remote_path: str) -> str:
    # Remove leading slash and normalize
    rel = remote_path.lstrip("/")
    return rel.replace("..", "__")


def _resolve_destination(dest_conn: Any, prefix: str, remote_path: str) -> DestURI:
    """
    Resolve destination URI based on connection type.

    For S3Connection: build s3://bucket/<base_path>/<prefix>/<remote_relpath>
    For FilesystemConnection: build file://<root>/<base>/<prefix>/<remote_relpath>
    """
    remote_rel = _safe_relpath(remote_path)
    prefix_clean = prefix.strip("/").strip()
    key_suffix = f"{prefix_clean}/{remote_rel}" if prefix_clean else remote_rel

    conn_type = getattr(dest_conn, "config", {}).get("type")
    # Note: get_connection returns connection wrapper instances (e.g. S3Connection)
    # which store original config on `.config`.
    if conn_type == "s3":
        cfg = dest_conn.config.get("config", {})
        bucket = cfg.get("bucket") or getattr(dest_conn, "bucket", "")
        base_path = cfg.get("base_path") or cfg.get("storage", {}).get("base_path") or ""
        key = f"{base_path.strip('/')}/{key_suffix}".strip("/")
        return DestURI(uri=f"s3://{bucket}/{key}", connection=dest_conn.name, key_or_path=key, bucket=bucket)

    if conn_type == "filesystem":
        root = str(dest_conn.root_path)
        base = str(dest_conn.base_path).strip("/")
        full = Path(root) / base / key_suffix
        return DestURI(uri=f"file://{full}", connection=dest_conn.name, key_or_path=str(full))

    raise ValueError(f"Unsupported destination connection type for sync: {conn_type}")


def _upload_to_destination(dest_conn: Any, dest: DestURI, local_path: str) -> None:
    conn_type = getattr(dest_conn, "config", {}).get("type")
    if conn_type == "s3":
        # Use S3Connection's upload_file method (handles atomicity internally)
        # Note: key_or_path already has base_path, so upload directly to client
        client = dest_conn.client
        tmp_key = f"{dest.key_or_path}.tmp"
        client.upload_file(local_path, dest.bucket or "", tmp_key)
        client.copy_object(
            Bucket=dest.bucket or "",
            CopySource={"Bucket": dest.bucket or "", "Key": tmp_key},
            Key=dest.key_or_path,
        )
        client.delete_object(Bucket=dest.bucket or "", Key=tmp_key)
        return

    if conn_type == "filesystem":
        full = Path(dest.key_or_path)
        full.parent.mkdir(parents=True, exist_ok=True)
        tmp = full.with_suffix(full.suffix + ".part")
        shutil.copy2(local_path, tmp)
        os.replace(tmp, full)
        return

    raise ValueError(f"Unsupported destination connection type for sync: {conn_type}")
