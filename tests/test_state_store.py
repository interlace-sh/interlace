"""SQLite state store: snapshots, the interval ledger, environments, durability."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from interlace.ir.relation import TableRef
from interlace.state.interval import Interval, IntervalSet
from interlace.state.snapshot import ChangeCategory, Snapshot
from interlace.state.store import SqliteStateStore, event_actor

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


async def test_events_for_run_selects_model_events_by_payload(tmp_path: Path) -> None:
    # A run's per-model events are keyed by payload.run (entity is the model name);
    # events_for_run must find them and ignore an apply's model events (no run key).
    store = await SqliteStateStore.open(tmp_path / "state.db")
    await store.append_event("run.started", entity="2", payload={"models": ["a"]})
    await store.append_event("model.start", entity="orders", payload={"run": 2})
    await store.append_event("model.done", entity="orders", payload={"run": 2})
    await store.append_event("model.start", entity="other", payload={"environment": "dev"})  # apply, not this run
    got = await store.events_for_run(2)
    await store.close()
    assert [(e["type"], e["entity"]) for e in got] == [("model.start", "orders"), ("model.done", "orders")]


async def test_run_id_column_is_backfilled_and_indexed(tmp_path: Path) -> None:
    """Rows written before run_id existed are still found, and trim/run scans have indexes."""
    import sqlite3

    from interlace.state.schema import _MIGRATIONS

    path = tmp_path / "state.db"
    conn = sqlite3.connect(path)
    try:
        for script in _MIGRATIONS[:-1]:
            for statement in script.split(";"):
                if statement.strip():
                    conn.execute(statement)
        conn.execute(f"PRAGMA user_version = {len(_MIGRATIONS) - 1}")
        conn.execute(
            "INSERT INTO event_log (ts, type, entity, payload) VALUES (?, ?, ?, ?)",
            ("2026-01-01T00:00:00", "model.done", "orders", '{"run": 7}'),
        )
        conn.commit()
    finally:
        conn.close()

    store = await SqliteStateStore.open(path)
    found = await store.events_for_run(7)
    await store.append_event("model.start", entity="orders", payload={"run": 7})
    again = await store.events_for_run(7)
    await store.close()
    assert [(event["type"], event["entity"]) for event in found] == [("model.done", "orders")]
    assert [event["type"] for event in again] == ["model.done", "model.start"]

    check = sqlite3.connect(path)
    try:
        names = {row[0] for row in check.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
    finally:
        check.close()
    assert {"idx_event_log_run_id", "idx_event_log_ts", "idx_check_results_executed_at"} <= names

    from interlace.state.schema import migration_followup

    sqlite_sql = migration_followup(13, "sqlite")
    postgres_sql = migration_followup(13, "postgres")
    assert sqlite_sql is not None and "json_extract(payload, '$.run')" in sqlite_sql
    assert postgres_sql is not None and "(payload::json ->> 'run')" in postgres_sql
    assert postgres_sql is not None and "json_extract" not in postgres_sql


async def test_list_runs_enriches_duration_and_environment(store: SqliteStateStore) -> None:
    """list_runs derives each run's wall-clock span and target env from its events,
    so the runs view can show duration + env without a per-run round-trip."""
    await store.enqueue_run("api:prod:abc", ["event_totals"], None)
    run_id = str((await store.list_runs())[0]["id"])
    await store.append_event("run.started", entity=run_id, payload={})
    await store.append_event("run.succeeded", entity=run_id, payload={"environment": "prod"})

    enriched = (await store.list_runs())[0]
    assert enriched["environment"] == "prod"
    assert enriched["started_at"] and enriched["finished_at"]  # both endpoints of the span recorded


async def test_ndjson_mirror_stamps_apply_actor(tmp_path: Path) -> None:
    store = await SqliteStateStore.open(tmp_path / "state.db")
    mirror = tmp_path / "events.ndjson"
    store.event_log_path = str(mirror)
    token = event_actor.set("cli")
    try:
        await store.append_event("apply.finished", entity="prod", payload={"built": 1})
        await store.append_event("stream.flushed", entity="orders", payload={"rows": 1})
    finally:
        event_actor.reset(token)
    stored = await store.read_events()
    await store.close()
    assert stored[0]["payload"] == {"built": 1, "api_key": "cli"}
    assert stored[1]["payload"] == {"rows": 1}
    lines = [json.loads(line) for line in mirror.read_text().splitlines()]
    assert [line["type"] for line in lines] == ["apply.finished", "stream.flushed"]
    assert lines[0]["payload"]["api_key"] == "cli"


async def test_list_runs_leaves_derived_fields_none_before_a_run(store: SqliteStateStore) -> None:
    await store.enqueue_run("api:prod:xyz", ["event_totals"], None)
    queued = (await store.list_runs())[0]
    assert queued["started_at"] is None and queued["finished_at"] is None and queued["environment"] is None
