"""SQLite state store: snapshots, the interval ledger, environments, durability."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from interlace.ir.relation import TableRef
from interlace.state.interval import Interval, IntervalSet
from interlace.state.snapshot import ChangeCategory, Snapshot
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


def d(day: int) -> datetime:
    return datetime(2026, 1, day)


def _snapshot(name: str = "silver.orders", fp: str = "aaaa1111", intervals: IntervalSet | None = None) -> Snapshot:
    return Snapshot(
        name=name,
        fingerprint=fp,
        metadata_hash="meta0001",
        physical_table=TableRef(schema="interlace__silver", name=f"orders__{fp}"),
        change_category=ChangeCategory.BREAKING,
        intervals=intervals if intervals is not None else IntervalSet([Interval(d(1), d(3))]),
    )


@pytest.fixture()
async def store(tmp_path: Path) -> SqliteStateStore:
    return await SqliteStateStore.open(tmp_path / "state.db")


async def test_add_and_get_snapshot_roundtrips(store: SqliteStateStore) -> None:
    snap = _snapshot()
    await store.add_snapshot(snap)

    loaded = await store.get_snapshot("silver.orders", "aaaa1111")
    assert loaded is not None
    assert loaded.fingerprint == "aaaa1111"
    assert loaded.metadata_hash == "meta0001"
    assert loaded.physical_table == TableRef(schema="interlace__silver", name="orders__aaaa1111")
    assert loaded.change_category is ChangeCategory.BREAKING
    assert list(loaded.intervals) == [Interval(d(1), d(3))]


async def test_get_missing_snapshot_is_none(store: SqliteStateStore) -> None:
    assert await store.get_snapshot("nope", "ffff") is None


async def test_list_snapshots_returns_all_versions(store: SqliteStateStore) -> None:
    await store.add_snapshot(_snapshot(fp="aaaa1111"))
    await store.add_snapshot(_snapshot(fp="bbbb2222"))

    versions = await store.list_snapshots("silver.orders")
    assert {s.fingerprint for s in versions} == {"aaaa1111", "bbbb2222"}


async def test_record_interval_extends_the_ledger(store: SqliteStateStore) -> None:
    await store.add_snapshot(_snapshot(intervals=IntervalSet([Interval(d(1), d(3))])))
    await store.record_interval("silver.orders", "aaaa1111", Interval(d(3), d(5)))

    # adjacent ranges merge when reconstructed as an IntervalSet
    assert list(await store.get_intervals("silver.orders", "aaaa1111")) == [Interval(d(1), d(5))]


async def test_add_snapshot_replaces_its_intervals(store: SqliteStateStore) -> None:
    await store.add_snapshot(_snapshot(intervals=IntervalSet([Interval(d(1), d(2))])))
    await store.add_snapshot(_snapshot(intervals=IntervalSet([Interval(d(8), d(9))])))

    assert list(await store.get_intervals("silver.orders", "aaaa1111")) == [Interval(d(8), d(9))]


async def test_promote_and_get_environment(store: SqliteStateStore) -> None:
    await store.promote("prod", {"silver.orders": "aaaa1111", "silver.customers": "cccc3333"})
    await store.promote("prod", {"silver.orders": "bbbb2222"})  # re-promote one model

    env = await store.get_environment("prod")
    assert env == {"silver.orders": "bbbb2222", "silver.customers": "cccc3333"}
    assert await store.get_environment("dev") == {}


async def test_state_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    store = await SqliteStateStore.open(path)
    await store.add_snapshot(_snapshot())
    await store.promote("prod", {"silver.orders": "aaaa1111"})
    await store.close()

    reopened = await SqliteStateStore.open(path)
    assert await reopened.get_snapshot("silver.orders", "aaaa1111") is not None
    assert await reopened.get_environment("prod") == {"silver.orders": "aaaa1111"}
    await reopened.close()
