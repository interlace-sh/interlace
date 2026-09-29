"""Cross-process advisory locks for warehouse-mutating work.

CLI ``apply`` / ``run`` and the daemon (HTTP apply, stream flush, scheduler drain,
gc, env drop/rollback) all share one SQLite state file. An in-process
``asyncio.Lock`` cannot serialise those writers — this module does, via
:meth:`AdvisoryLockStore.acquire_lock` with a heartbeat while the critical
section runs.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from interlace.exceptions import LockError
from interlace.state.beat import start_heartbeat, stop_heartbeat
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
    acquired = await store.locks.acquire_lock(APPLY_LOCK, owner=owner, lease_seconds=lease_seconds, timeout=timeout)
    if not acquired:
        raise LockError(
            f"could not acquire the apply lock within {timeout:.0f}s — "
            "another process is applying, flushing streams, or draining runs",
            details={"lock": APPLY_LOCK, "owner": owner},
        )
    # Off the event loop, same reason as the run lease: a model that blocks the
    # loop must not drop this lock and let a second process start another apply.
    stop, beat = start_heartbeat(
        "interlace-apply-lock",
        max(lease_seconds / 3.0, 0.05),
        lambda: store.locks._renew_lock_sync(APPLY_LOCK, owner, lease_seconds),
    )
    try:
        yield
    finally:
        stop_heartbeat(stop, beat)
        await store.locks.release_lock(APPLY_LOCK, owner=owner)
