"""Per-task worker: leases + crash reclaim, durable retries, timeouts, and
cooperative cancellation through the heartbeat channel."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.scheduler.worker import drain
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


@pytest.fixture()
async def store(tmp_path: Path) -> AsyncIterator[SqliteStateStore]:
    s = await SqliteStateStore.open(tmp_path / "state.db")
    yield s
    await s.close()


async def _state_of(store: SqliteStateStore, run_id: int) -> dict:
    return next(r for r in await store.list_runs() if r["id"] == run_id)


async def test_restate_flag_rides_the_queue(store: SqliteStateStore) -> None:
    await store.enqueue_run("k-restate", ["m"], ("2026-07-01T00:00:00", "2026-07-02T00:00:00"), 0, restate=True)
    await store.enqueue_run("k-plain", ["m"], None, 0)
    claimed = await store.claim_runs(owner="w")
    by_partition = {run.partition_start: run.restate for run in claimed}
    assert by_partition == {"2026-07-01T00:00:00": True, None: False}


# --- lease mechanics -------------------------------------------------------------


async def test_expired_lease_is_reclaimed_by_another_worker(store: SqliteStateStore) -> None:
    await store.enqueue_run("k1", ["m"], None, 0)
    first = await store.claim_runs(owner="worker-a", lease_seconds=0.0)  # lease dead on arrival
    assert [r.attempts for r in first] == [1]

    await asyncio.sleep(0.01)
    second = await store.claim_runs(owner="worker-b", lease_seconds=60.0)
    assert [r.attempts for r in second] == [2]  # reclaimed, attempt counted

    held = await store.claim_runs(owner="worker-c", lease_seconds=60.0)
    assert held == []  # b's live lease protects the run


async def test_reclaim_past_max_attempts_fails_the_run(store: SqliteStateStore) -> None:
    await store.enqueue_run("k1", ["m"], None, 0)
    for _ in range(3):  # three dead workers burn the attempts
        assert await store.claim_runs(owner="w", lease_seconds=0.0)
        await asyncio.sleep(0.01)

    final = await store.claim_runs(owner="w", lease_seconds=0.0, max_attempts=3)
    assert final == []
    run = (await store.list_runs())[0]
    assert run["state"] == "failed" and "retries exhausted" in str(run["error"])


async def test_renew_lease_verdicts(store: SqliteStateStore) -> None:
    await store.enqueue_run("k1", ["m"], None, 0)
    (run,) = await store.claim_runs(owner="me", lease_seconds=60.0)

    assert await store.renew_lease(run.id, owner="me") == "ok"
    assert await store.renew_lease(run.id, owner="impostor") == "lost"
    await store.request_cancel(run.id)
    assert await store.renew_lease(run.id, owner="me") == "cancel"


async def test_cancel_queued_run_is_immediate(store: SqliteStateStore) -> None:
    await store.enqueue_run("k1", ["m"], None, 0)
    run = (await store.list_runs())[0]
    assert await store.request_cancel(run["id"]) == "cancelled"
    assert (await _state_of(store, run["id"]))["state"] == "cancelled"
    assert await store.request_cancel(run["id"]) is None  # already finished


# --- drain behaviour ---------------------------------------------------------------


async def test_failing_run_retries_then_fails(store: SqliteStateStore) -> None:
    compiled = compile_models([ModelDef(name="broken", sql="SELECT * FROM does_not_exist")])
    engine = DuckDBAdapter.in_memory()
    await store.enqueue_run("k1", ["broken"], None, 0)

    await drain(store, compiled, engine, max_attempts=2)  # attempt 1: requeued
    assert (await store.list_runs())[0]["state"] == "queued"
    await drain(store, compiled, engine, max_attempts=2)  # attempt 2: exhausted
    run = (await store.list_runs())[0]
    assert run["state"] == "failed" and "does_not_exist" in str(run["error"])

    types = [e["type"] for e in await store.events_for_entity(str(run["id"]))]
    assert types.count("run.started") == 2 and "run.retrying" in types and "run.failed" in types
    engine.close()


async def test_timeout_bounds_an_attempt(store: SqliteStateStore) -> None:
    async def sleepy() -> object:
        import pyarrow as pa

        await asyncio.sleep(5)
        return pa.table({"x": [1]})

    compiled = compile_models([ModelDef(name="sleepy", fn=sleepy)])
    engine = DuckDBAdapter.in_memory()
    await store.enqueue_run("k1", ["sleepy"], None, 0)

    await drain(store, compiled, engine, task_timeout=0.1, max_attempts=1)
    run = (await store.list_runs())[0]
    assert run["state"] == "failed" and "timed out" in str(run["error"])
    engine.close()


async def test_running_run_cancels_cooperatively(store: SqliteStateStore) -> None:
    started = asyncio.Event()

    async def slow() -> object:
        import pyarrow as pa

        started.set()
        await asyncio.sleep(30)
        return pa.table({"x": [1]})

    compiled = compile_models([ModelDef(name="slow", fn=slow)])
    engine = DuckDBAdapter.in_memory()
    await store.enqueue_run("k1", ["slow"], None, 0)

    async def cancel_once_running() -> None:
        await started.wait()
        run = (await store.list_runs())[0]
        assert await store.request_cancel(run.get("id")) == "cancelling"

    # tiny lease -> heartbeat every ~0.05s sees the cancel flag quickly
    await asyncio.gather(
        drain(store, compiled, engine, lease_seconds=0.15),
        cancel_once_running(),
    )
    run = (await store.list_runs())[0]
    assert run["state"] == "cancelled"
    types = [e["type"] for e in await store.events_for_entity(str(run["id"]))]
    assert "run.cancelled" in types
    engine.close()


async def test_terminal_writes_are_fenced_by_lease_owner(store: SqliteStateStore) -> None:
    """A starved worker whose run was reclaimed cannot stomp the reclaimer's state."""
    await store.enqueue_run("k1", ["m"], None, 0)
    await store.claim_runs(owner="worker-a", lease_seconds=0.0)  # a's lease dies instantly
    await asyncio.sleep(0.01)
    await store.claim_runs(owner="worker-b", lease_seconds=60.0)  # b reclaims

    landed = await store.finish_run(1, success=True, owner="worker-a")  # a wakes up late
    assert landed is False
    assert (await store.list_runs())[0]["state"] == "running"  # b's run untouched

    assert await store.finish_run(1, success=True, owner="worker-b") is True
    assert (await store.list_runs())[0]["state"] == "succeeded"


async def test_requeue_is_fenced_too(store: SqliteStateStore) -> None:
    await store.enqueue_run("k1", ["m"], None, 0)
    await store.claim_runs(owner="worker-a", lease_seconds=0.0)
    await asyncio.sleep(0.01)
    await store.claim_runs(owner="worker-b", lease_seconds=60.0)

    assert await store.requeue_run(1, error="a is late", owner="worker-a") is False
    assert (await store.list_runs())[0]["state"] == "running"
