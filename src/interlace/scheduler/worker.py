"""Worker — drains the durable run queue and executes each run.

Claims queued runs and, for each, builds and applies a forced plan for its model
selection (so cron/interval-driven runs pick up new data, like ``interlace run``).
A failing run is recorded as failed rather than crashing the loop.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from interlace.engines.base import EngineAdapter
from interlace.graph.project import CompiledProject
from interlace.plan.apply import apply
from interlace.plan.run import run_plan
from interlace.state.store import SqliteStateStore


async def drain(
    store: SqliteStateStore,
    project: CompiledProject,
    engine: EngineAdapter,
    environment: str,
    *,
    base_path: Path | None = None,
    limit: int = 10,
) -> int:
    """Execute up to ``limit`` queued runs; returns how many were processed."""
    runs = await store.claim_runs(limit)
    for run in runs:
        await store.append_event("run.started", entity=str(run.id), payload={"models": run.flow_selector})
        try:
            start = datetime.fromisoformat(run.partition_start) if run.partition_start else None
            end = datetime.fromisoformat(run.partition_end) if run.partition_end else None
            plan = await run_plan(project, environment, store, start=start, end=end, select=set(run.flow_selector))
            result = await apply(plan, compiled=project, engine=engine, state=store, base_path=base_path)
            await store.finish_run(run.id, success=True)
            await store.append_event("run.succeeded", entity=str(run.id), payload={"built": result.built})
        except Exception as exc:  # a bad run must not kill the worker loop
            await store.finish_run(run.id, success=False, error=str(exc))
            await store.append_event("run.failed", entity=str(run.id), payload={"error": str(exc)})
    return len(runs)
