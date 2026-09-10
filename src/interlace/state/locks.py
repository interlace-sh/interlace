"""Cross-process advisory locks for warehouse-mutating work.

CLI ``apply`` / ``run`` and the daemon (HTTP apply, stream flush, scheduler drain,
gc, env drop/rollback) all share one SQLite state file. An in-process
``asyncio.Lock`` cannot serialise those writers — this module does, via
:meth:`SqliteStateStore.acquire_lock` with a heartbeat while the critical
section runs.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from interlace.exceptions import LockError
from interlace.state.store import SqliteStateStore

APPLY_LOCK = "apply"


@asynccontextmanager
async def hold_apply_lock(
    store: SqliteStateStore,
    *,
    owner: str,
    lease_seconds: float = 180.0,
    timeout: float = 60.0,
) -> AsyncIterator[None]:
    """Hold the warehouse ``apply`` lock until the block exits; renew while held."""
    acquired = await store.acquire_lock(APPLY_LOCK, owner=owner, lease_seconds=lease_seconds, timeout=timeout)
    if not acquired:
        raise LockError(
            f"could not acquire the apply lock within {timeout:.0f}s — "
            "another process is applying, flushing streams, or draining runs",
            details={"lock": APPLY_LOCK, "owner": owner},
        )
    stop = asyncio.Event()

    async def _heartbeat() -> None:
        interval = max(1.0, lease_seconds / 3.0)
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except TimeoutError:
                if not await store.renew_lock(APPLY_LOCK, owner=owner, lease_seconds=lease_seconds):
                    return  # lost the lock; the holder may fail on its own

    beat = asyncio.create_task(_heartbeat())
    try:
        yield
    finally:
        stop.set()
        beat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await beat
        await store.release_lock(APPLY_LOCK, owner=owner)
