"""Scheduling core: triggers, the trigger engine, the work queue, and the worker."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest
import sqlglot
from typer.testing import CliRunner

from interlace.cli.main import app
from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.scheduler.engine import TriggerEngine, build_triggers
from interlace.scheduler.triggers import CronTrigger, IntervalTrigger
from interlace.scheduler.worker import drain
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit

runner = CliRunner()
EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "getting_started"


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[tuple[DuckDBAdapter, SqliteStateStore]]:
    engine = DuckDBAdapter.in_memory()
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield engine, store
    await store.close()
    engine.close()


def test_cron_trigger_due_only_after_a_scheduled_time() -> None:
    trigger = CronTrigger("m", "0 * * * *")  # top of every hour
    assert trigger.due(datetime(2026, 1, 1, 10, 0), datetime(2026, 1, 1, 9, 0))  # 10:00 reached
    assert not trigger.due(datetime(2026, 1, 1, 10, 30), datetime(2026, 1, 1, 10, 0))  # mid-hour, already fired


def test_interval_trigger_fires_on_first_sight_then_every() -> None:
    trigger = IntervalTrigger("m", timedelta(minutes=5))
    now = datetime(2026, 1, 1, 12, 0)
    assert trigger.due(now, None)  # first sight
    assert not trigger.due(now, now - timedelta(minutes=3))
    assert trigger.due(now, now - timedelta(minutes=6))


async def test_engine_tick_enqueues_then_dedupes(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    _, store = env
    project = compile_models([ModelDef(name="m", sql="SELECT 1 AS x", schedule={"every": "1h"})])
    engine = TriggerEngine(build_triggers(project), store)

    now = datetime(2026, 1, 1, 12, 0)
    assert await engine.tick(now) == 1  # first tick enqueues
    assert await engine.tick(now) == 0  # same tick: last_fired advanced, nothing new
    assert await store.count_pending_runs() == 1


async def test_worker_drains_and_executes_a_run(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([ModelDef(name="m", sql="SELECT 7 AS x")])
    await store.enqueue_run("k1", ["m"], None, 0)

    processed = await drain(store, project, engine, "prod")
    assert processed == 1
    assert await store.count_pending_runs() == 0

    reader = await engine.fetch(sqlglot.parse_one("SELECT x FROM main.m"))
    assert reader.read_all().to_pylist() == [{"x": 7}]


async def test_event_log_append_and_read(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    _, store = env
    s1 = await store.append_event("run.enqueued", entity="k", payload={"models": ["m"]})
    s2 = await store.append_event("run.started", entity="1")
    assert s2 > s1

    events = await store.read_events(after_seq=0)
    assert [e["type"] for e in events] == ["run.enqueued", "run.started"]
    assert events[0]["payload"] == {"models": ["m"]}
    assert [e["type"] for e in await store.read_events(after_seq=s1)] == ["run.started"]  # replay from a cursor


async def test_worker_emits_run_lifecycle_events(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([ModelDef(name="m", sql="SELECT 1 AS x")])
    await store.enqueue_run("k1", ["m"], None, 0)
    await drain(store, project, engine, "prod")

    types = [e["type"] for e in await store.read_events()]
    assert "run.started" in types
    assert "run.succeeded" in types


async def test_enqueue_is_idempotent(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    _, store = env
    assert await store.enqueue_run("dup", ["m"], None, 0) is True
    assert await store.enqueue_run("dup", ["m"], None, 0) is False  # same key, not re-queued
    assert await store.count_pending_runs() == 1


def test_scheduler_once_builds_a_scheduled_model(tmp_path: Path) -> None:
    project_dir = tmp_path / "proj"
    (project_dir / "models").mkdir(parents=True)
    (project_dir / "interlace.yaml").write_text("name: sched\n")
    (project_dir / "models" / "m.sql").write_text("/*\ninterlace:\n  schedule:\n    every: 1s\n*/\nSELECT 5 AS x")

    result = runner.invoke(app, ["scheduler", "--env", "dev", "--path", str(project_dir), "--once"])
    assert result.exit_code == 0, result.output

    con = duckdb.connect(f"ducklake:{project_dir / '.interlace' / 'warehouse.ducklake'}")
    try:
        assert con.execute("SELECT x FROM dev__main.m").fetchone() == (5,)
    finally:
        con.close()
