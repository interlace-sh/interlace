"""
Run one SFTP sync tick for the demo project.
"""

from __future__ import annotations

from pathlib import Path

from interlace.config.loader import load_config
from interlace.connections.manager import init_connections
from interlace.core.state import StateStore
from interlace.sync.sftp_sync import run_sftp_sync_job
from interlace.sync.types import SFTPSyncJob, ProcessorSpec


def _job_from_config(job_dict: dict) -> SFTPSyncJob:
    processors = [ProcessorSpec(name=p["name"], config=p.get("config")) for p in job_dict.get("processors", [])]
    return SFTPSyncJob(
        job_id=job_dict["job_id"],
        sftp_connection=job_dict["sftp_connection"],
        remote_glob=job_dict["remote_glob"],
        destination_connection=job_dict["destination_connection"],
        destination_prefix=job_dict.get("destination_prefix", ""),
        processors=processors,
        max_files_per_run=int(job_dict.get("max_files_per_run", 200)),
        change_detection=str(job_dict.get("change_detection", "mtime_size")),
        work_dir=str(job_dict.get("work_dir", "/tmp/interlace-sync")),
    )


def main() -> None:
    project_dir = Path(__file__).parent
    config_obj = load_config(project_dir, env=None)
    config = config_obj.data
    init_connections(config)
    state = StateStore(config)

    jobs = config.get("sync", {}).get("jobs", [])
    if not jobs:
        raise SystemExit("No sync.jobs configured")

    job = _job_from_config(jobs[0])
    summary = run_sftp_sync_job(job, config=config, state_store=state)
    print(summary)


if __name__ == "__main__":
    main()

