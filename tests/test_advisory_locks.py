"""Cross-process advisory locks for warehouse-mutating work."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from interlace.exceptions import LockError
from interlace.state.locks import APPLY_LOCK, hold_apply_lock
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


@pytest.fixture()
async def store(tmp_path: Path) -> AsyncIterator[SqliteStateStore]:
    s = await SqliteStateStore.open(tmp_path / "state.db")
    yield s
    await s.close()


async def test_acquire_blocks_a_second_owner(store: SqliteStateStore) -> None:
    assert await store.acquire_lock(APPLY_LOCK, owner="a", lease_seconds=60.0, timeout=0.0)
    assert not await store.acquire_lock(APPLY_LOCK, owner="b", lease_seconds=60.0, timeout=0.05)
    await store.release_lock(APPLY_LOCK, owner="a")
    assert await store.acquire_lock(APPLY_LOCK, owner="b", lease_seconds=60.0, timeout=0.0)


async def test_same_owner_may_reacquire(store: SqliteStateStore) -> None:
    assert await store.acquire_lock(APPLY_LOCK, owner="a", lease_seconds=60.0, timeout=0.0)
    assert await store.acquire_lock(APPLY_LOCK, owner="a", lease_seconds=60.0, timeout=0.0)


async def test_expired_lock_is_stealable(store: SqliteStateStore) -> None:
    assert await store.acquire_lock(APPLY_LOCK, owner="a", lease_seconds=0.0, timeout=0.0)  # already expired
    assert await store.acquire_lock(APPLY_LOCK, owner="b", lease_seconds=60.0, timeout=0.0)


async def test_hold_apply_lock_times_out(store: SqliteStateStore) -> None:
    assert await store.acquire_lock(APPLY_LOCK, owner="other", lease_seconds=60.0, timeout=0.0)
    with pytest.raises(LockError, match="could not acquire"):
        async with hold_apply_lock(store, owner="me", timeout=0.05):
            pass  # pragma: no cover
