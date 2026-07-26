"""Worker — drains the durable run queue with leases, retries, and cancellation.

Each claimed run holds a **lease**: a heartbeat task renews it while the run
executes, so a crashed worker's runs are reclaimed by the next `claim_runs`
once the lease expires (and marked failed once retries are exhausted). The
heartbeat doubles as the **cooperative cancellation** channel — a cancel
request flips a flag the next heartbeat sees, which cancels the executing task
and records the run as ``cancelled``. Failures requeue for a durable retry
until ``max_attempts``; ``task_timeout`` bounds a single attempt. Runs execute
concurrently up to ``slots`` (the DAG's per-apply ordering still holds inside
each run).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
from datetime import datetime
from pathlib import Path

from interlace.engines.base import EngineAdapter
from interlace.engines.registry import EngineRegistry
from interlace.graph.project import CompiledProject
from interlace.plan.apply import apply
from interlace.plan.run import run_plan
from interlace.state.store import QueuedRun, SqliteStateStore


def default_owner() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


async def drain(
    store: SqliteStateStore,
    project: CompiledProject,
    engine: EngineAdapter | None = None,
    environment: str = "prod",
    *,
    engines: EngineRegistry | dict[str, EngineAdapter] | None = None,
    base_path: Path | None = None,
    limit: int = 10,
    owner: str | None = None,
    lease_seconds: float = 60.0,
    max_attempts: int = 3,
    task_timeout: float | None = None,
    slots: int = 1,
) -> int:
    """Execute up to ``limit`` queued (or lease-expired) runs; returns how many ran."""
    worker = owner or default_owner()
    runs = await store.claim_runs(limit, owner=worker, lease_seconds=lease_seconds, max_attempts=max_attempts)
    semaphore = asyncio.Semaphore(max(1, slots))

    async def bounded(run: QueuedRun) -> None:
        async with semaphore:
            await _execute_run(
                run,
                store,
                project,
                engine,
                environment,
                engines=engines,
                base_path=base_path,
                owner=worker,
                lease_seconds=lease_seconds,
                max_attempts=max_attempts,
                task_timeout=task_timeout,
            )

    if runs:
        await asyncio.gather(*(bounded(run) for run in runs))
    return len(runs)


async def _execute_run(
    run: QueuedRun,
    store: SqliteStateStore,
    project: CompiledProject,
    engine: EngineAdapter | None,
    environment: str,
    *,
    engines: EngineRegistry | dict[str, EngineAdapter] | None,
    base_path: Path | None,
    owner: str,
    lease_seconds: float,
    max_attempts: int,
    task_timeout: float | None,
) -> None:
    await store.append_event(
        "run.started", entity=str(run.id), payload={"models": run.flow_selector, "attempt": run.attempts}
    )
    cancelled = asyncio.Event()

    async def heartbeat() -> None:
        interval = max(lease_seconds / 3.0, 0.05)
        while True:
            await asyncio.sleep(interval)
            verdict = await store.renew_lease(run.id, owner=owner, lease_seconds=lease_seconds)
            if verdict != "ok":
                cancelled.set()  # cancel requested, or another worker reclaimed the lease
                return

    async def execute() -> dict[str, object]:
        start = datetime.fromisoformat(run.partition_start) if run.partition_start else None
        end = datetime.fromisoformat(run.partition_end) if run.partition_end else None
        plan = await run_plan(project, environment, store, start=start, end=end, select=set(run.flow_selector))
        result = await apply(plan, compiled=project, engine=engine, engines=engines, state=store, base_path=base_path)
        return {
            "built": result.built,
            "timings": {name: round(seconds, 3) for name, seconds in result.timings.items()},
        }

    beat = asyncio.create_task(heartbeat())
    work = asyncio.create_task(execute())
    watcher = asyncio.create_task(cancelled.wait())
    try:
        done, _ = await asyncio.wait({work, watcher}, timeout=task_timeout, return_when=asyncio.FIRST_COMPLETED)
        if work in done:
            payload = work.result()  # raises the run's own error if it failed
            await store.finish_run(run.id, success=True)
            await store.append_event("run.succeeded", entity=str(run.id), payload=payload)
        elif watcher in done:  # cooperative cancellation (or lost lease)
            work.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await work
            await store.finish_run(run.id, success=False, error="cancelled", status="cancelled")
            await store.append_event("run.cancelled", entity=str(run.id), payload={})
        else:  # timeout: the attempt is abandoned; retry policy decides what's next
            work.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await work
            await _fail_or_retry(store, run, f"timed out after {task_timeout}s", max_attempts)
    except Exception as exc:  # a bad run must not kill the worker loop
        await _fail_or_retry(store, run, str(exc), max_attempts)
    finally:
        beat.cancel()
        watcher.cancel()
        for task in (beat, watcher):
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def _fail_or_retry(store: SqliteStateStore, run: QueuedRun, error: str, max_attempts: int) -> None:
    if run.attempts < max_attempts:
        await store.requeue_run(run.id, error=error)
        await store.append_event("run.retrying", entity=str(run.id), payload={"error": error, "attempt": run.attempts})
    else:
        await store.finish_run(run.id, success=False, error=error)
        await store.append_event("run.failed", entity=str(run.id), payload={"error": error})
