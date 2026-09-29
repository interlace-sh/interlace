"""Worker — drains the durable run queue with leases, retries, and cancellation.

Each claimed run holds a **lease**: a thread renews it while the run executes,
so a crashed worker's runs are reclaimed by the next `claim_runs` once the
lease expires (and marked failed once retries are exhausted). The lease is
that crash window — one minute by default — not a limit on how long a model
may run. A model can take hours; the thread keeps renewing even when the
model blocks the event loop. The heartbeat doubles as the **cooperative
cancellation** channel — a cancel request flips a flag the next heartbeat
sees, which cancels the executing task and records the run as ``cancelled``.
There is no runtime cap unless ``task_timeout`` is set. Failures requeue for
a durable retry until ``max_attempts``. A retry rebuilds only models that did
not reach ``model.done``; the ones that finished are promoted again and not
recomputed. Runs execute concurrently up to ``slots`` (the DAG's per-apply
ordering still holds inside each run).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from interlace.dsl.dynamic import apply_with_registrations
from interlace.engines.base import EngineAdapter
from interlace.engines.registry import EngineRegistry
from interlace.graph.project import CompiledProject
from interlace.plan.apply import apply
from interlace.plan.run import run_plan
from interlace.project import Project
from interlace.state.beat import start_heartbeat, stop_heartbeat
from interlace.state.store import QueuedRun, SqliteStateStore, event_actor

logger = logging.getLogger("interlace.worker")


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
    parallelism: int = 4,
    connections: Mapping[str, Any] | None = None,
    loaded: Project | None = None,
    on_compiled: Callable[[CompiledProject], None] | None = None,
) -> int:
    """Execute up to ``limit`` queued (or lease-expired) runs; returns how many ran."""
    actor = event_actor.set("scheduler")
    try:
        worker = owner or default_owner()
        runs = await store.claim_runs(limit, owner=worker, lease_seconds=lease_seconds, max_attempts=max_attempts)
        semaphore = asyncio.Semaphore(max(1, slots))

        async def bounded(run: QueuedRun) -> None:
            async with semaphore:
                # the lease was taken at CLAIM time; a run that queued behind a long
                # sibling may have expired and been reclaimed — re-verify before
                # executing, or two workers run the same plan for a heartbeat window
                verdict = await store.renew_lease(run.id, owner=worker, lease_seconds=lease_seconds)
                if verdict == "lost":
                    return
                if verdict == "cancel":
                    if await store.finish_run(
                        run.id, success=False, error="cancelled", status="cancelled", owner=worker
                    ):
                        await store.append_event("run.cancelled", entity=str(run.id), payload={})
                    return
                await _execute_run(
                    run,
                    store,
                    project,
                    engine,
                    environment,
                    engines=engines,
                    base_path=base_path,
                    connections=connections,
                    loaded=loaded,
                    on_compiled=on_compiled,
                    owner=worker,
                    lease_seconds=lease_seconds,
                    max_attempts=max_attempts,
                    task_timeout=task_timeout,
                    parallelism=parallelism,
                )

        if runs:
            await asyncio.gather(*(bounded(run) for run in runs))
        return len(runs)
    finally:
        event_actor.reset(actor)


async def _execute_run(  # noqa: C901
    run: QueuedRun,
    store: SqliteStateStore,
    project: CompiledProject,
    engine: EngineAdapter | None,
    environment: str,
    *,
    engines: EngineRegistry | dict[str, EngineAdapter] | None,
    base_path: Path | None,
    connections: Mapping[str, Any] | None,
    loaded: Project | None,
    on_compiled: Callable[[CompiledProject], None] | None,
    owner: str,
    lease_seconds: float,
    max_attempts: int,
    task_timeout: float | None,
    parallelism: int = 4,
) -> None:
    await store.append_event(
        "run.started", entity=str(run.id), payload={"models": run.flow_selector, "attempt": run.attempts}
    )
    logger.info("run %s started (attempt %s): %s", run.id, run.attempts, ", ".join(run.flow_selector) or "all")
    cancelled = asyncio.Event()
    loop = asyncio.get_running_loop()

    def renew() -> bool:
        verdict = store.renew_lease_sync(run.id, owner=owner, lease_seconds=lease_seconds)
        if verdict == "ok":
            return True
        if verdict == "lost":
            logger.error("run %s lost its lease to another worker", run.id)
        try:
            loop.call_soon_threadsafe(cancelled.set)
        except RuntimeError:
            return False
        return False

    stop_beat, beat = start_heartbeat(f"interlace-lease-{run.id}", max(lease_seconds / 3.0, 0.05), renew)

    async def execute() -> dict[str, object]:
        start = datetime.fromisoformat(run.partition_start) if run.partition_start else None
        end = datetime.fromisoformat(run.partition_end) if run.partition_end else None
        finished = await _finished_models(store, run.id)
        if finished:
            logger.info("run %s resuming; already built %s", run.id, ", ".join(sorted(finished)))
        plan = await run_plan(
            project,
            environment,
            store,
            start=start,
            end=end,
            select=set(run.flow_selector),
            restate=run.restate,
            already_built=finished,
        )
        loop = asyncio.get_running_loop()
        background: set[asyncio.Task] = set()

        def on_progress(model: str, event: str, detail: dict | None = None) -> None:
            # fire-and-forget telemetry — but hold a strong ref: an unreferenced
            # task can be GC'd mid-write and its exception silently vanishes
            payload: dict = {"run": run.id, **(detail or {})}
            task = loop.create_task(store.append_event(f"model.{event}", entity=model, payload=payload))
            background.add(task)
            task.add_done_callback(background.discard)

        if loaded is None:
            result = await apply(
                plan,
                compiled=project,
                engine=engine,
                engines=engines,
                state=store,
                base_path=base_path,
                parallelism=parallelism,
                on_progress=on_progress,
                connections=connections,
            )
        else:
            result = await apply_with_registrations(
                plan,
                compiled=project,
                project=loaded,
                on_compiled=on_compiled,
                engine=engine,
                engines=engines,
                state=store,
                base_path=base_path,
                parallelism=parallelism,
                on_progress=on_progress,
                connections=connections,
            )
        if background:  # let the progress events land before the run is marked done
            await asyncio.gather(*background, return_exceptions=True)
        return {
            "built": result.built,
            "reused": result.reused,
            "gated": result.gated,
            "promoted": result.promoted,
            "environment": environment,
            "timings": {name: round(seconds, 3) for name, seconds in result.timings.items()},
            "rows": {
                name: {"inserted": c.inserted, "updated": c.updated, "deleted": c.deleted}
                for name, c in result.rows.items()
            },
            "checks": {
                "passed": sum(1 for c in result.checks if c.status == "passed"),
                "total": len(result.checks),
                "failing": [f"{c.model}.{c.name}" for c in result.checks if c.status != "passed"],
            },
        }

    work = asyncio.create_task(execute())
    watcher = asyncio.create_task(cancelled.wait())
    try:
        done, _ = await asyncio.wait({work, watcher}, timeout=task_timeout, return_when=asyncio.FIRST_COMPLETED)
        if work in done:
            payload = work.result()  # raises the run's own error if it failed
            if await store.finish_run(run.id, success=True, owner=owner):
                await store.append_event("run.succeeded", entity=str(run.id), payload=payload)
                logger.info("run %s succeeded: built %s", run.id, ", ".join(payload["built"]) or "nothing")  # type: ignore[arg-type]
        elif watcher in done:  # cooperative cancellation (or lost lease)
            work.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await work
            if await store.finish_run(run.id, success=False, error="cancelled", status="cancelled", owner=owner):
                await store.append_event("run.cancelled", entity=str(run.id), payload={})
        else:  # timeout: the attempt is abandoned; retry policy decides what's next
            work.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await work
            await _fail_or_retry(store, run, f"timed out after {task_timeout}s", max_attempts, owner)
    except Exception as exc:  # a bad run must not kill the worker loop
        await _fail_or_retry(store, run, str(exc), max_attempts, owner)
    finally:
        stop_heartbeat(stop_beat, beat)
        watcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher


async def _finished_models(store: SqliteStateStore, run_id: int) -> set[str]:
    """Models that reached ``model.done`` on an earlier attempt of this run."""
    events = await store.events_for_run(run_id)
    return {str(event["entity"]) for event in events if event.get("type") == "model.done" and event.get("entity")}


async def _fail_or_retry(store: SqliteStateStore, run: QueuedRun, error: str, max_attempts: int, owner: str) -> None:
    # fenced: if another worker reclaimed the lease, its outcome wins — stay silent
    logger.warning("run %s attempt %s failed: %s", run.id, run.attempts, error)
    if run.attempts < max_attempts:
        if await store.requeue_run(run.id, error=error, owner=owner):
            await store.append_event(
                "run.retrying", entity=str(run.id), payload={"error": error, "attempt": run.attempts}
            )
    elif await store.finish_run(run.id, success=False, error=error, owner=owner):
        await store.append_event("run.failed", entity=str(run.id), payload={"error": error})
