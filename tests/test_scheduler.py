"""Scheduling core: triggers, the trigger engine, the work queue, and the worker."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
import sqlglot
from typer.testing import CliRunner

from interlace.cli.main import app
from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import DefinitionError
from interlace.graph.project import compile_models
from interlace.project import Project
from interlace.scheduler.daemon import startup_apply
from interlace.scheduler.engine import TriggerEngine, build_triggers, scheduled_closure
from interlace.scheduler.triggers import CronTrigger, IntervalTrigger
from interlace.scheduler.worker import _finished_models, drain
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit

runner = CliRunner()
EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "getting_started"


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


def test_interval_trigger_key_is_stable_within_a_slot() -> None:
    """Crash between enqueue and the last-fired write: the retry after restart
    must land on the SAME idempotency key so the durable queue dedupes it."""
    trigger = IntervalTrigger("m", timedelta(minutes=5))
    first = trigger.due(datetime(2026, 1, 1, 12, 0, 1), None)[0]
    retry = trigger.due(datetime(2026, 1, 1, 12, 3, 59), None)[0]  # restarted, same 5-min slot
    assert first.idempotency_key == retry.idempotency_key
    later = trigger.due(datetime(2026, 1, 1, 12, 5, 1), None)[0]  # next slot: a new firing
    assert later.idempotency_key != first.idempotency_key


async def test_watch_trigger_enqueues_on_change_and_dedupes(
    env: tuple[DuckDBAdapter, SqliteStateStore], tmp_path: Path
) -> None:
    _, store = env
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "a.csv").write_text("id\n1\n")
    project = compile_models([ModelDef(name="m", sql="SELECT 1 AS x", schedule={"watch": "inbox/*.csv"})])
    engine = TriggerEngine(build_triggers(project, root=tmp_path), store, project)
    now = datetime(2026, 1, 1, 12, 0)
    assert await engine.tick(now) == 1
    assert await engine.tick(now) == 0  # same path, size, and mtime
    (inbox / "a.csv").write_text("id\n1\n2\n")
    assert await engine.tick(now) == 1
    assert await store.count_pending_runs() == 2


async def test_on_change_enqueues_when_the_column_max_moves(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    engine, store = env
    await engine.execute_sql("CREATE TABLE events (id INTEGER, updated_at INTEGER)")
    await engine.execute_sql("INSERT INTO events VALUES (1, 10)")
    project = compile_models(
        [
            ModelDef(name="m", sql="SELECT id FROM events", schedule={"on_change": "updated_at"}),
            ModelDef(name="mart", sql="SELECT id FROM m"),
        ]
    )
    trigger = TriggerEngine(build_triggers(project), store, project, engines=engine)
    now = datetime(2026, 1, 1, 12, 0)
    assert await trigger.tick(now) == 1
    assert await trigger.tick(now) == 0  # same max
    await engine.execute_sql("UPDATE events SET updated_at = 11")
    assert await trigger.tick(now) == 1
    runs = await store.list_runs()
    assert len(runs) == 2
    assert all(set(run["flow_selector"]) == {"m", "mart"} for run in runs)


async def test_on_change_waits_until_the_source_table_exists(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    engine, store = env
    project = compile_models([ModelDef(name="m", sql="SELECT id FROM absent", schedule={"on_change": "updated_at"})])
    trigger = TriggerEngine(build_triggers(project), store, project, engines=engine)
    assert await trigger.tick(datetime(2026, 1, 1, 12, 0)) == 0


async def test_on_change_reads_an_upstream_model_view(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    raw = ModelDef(name="raw", sql="SELECT 1 AS id, 10 AS updated_at")
    await store.enqueue_run("setup", ["raw"], None, 0)
    await drain(store, compile_models([raw]), engine, "prod")
    project = compile_models(
        [raw, ModelDef(name="mart", sql="SELECT updated_at FROM raw", schedule={"on_change": "updated_at"})]
    )
    trigger = TriggerEngine(build_triggers(project, environment="prod"), store, project, engines=engine)
    assert await trigger.tick(datetime(2026, 1, 1, 12, 0)) == 1
    runs = await store.list_runs()
    assert set(runs[0]["flow_selector"]) == {"mart"}


def test_on_change_needs_one_source_table() -> None:
    project = compile_models(
        [ModelDef(name="m", sql="SELECT 1 FROM a CROSS JOIN b", schedule={"on_change": "updated_at"})]
    )
    with pytest.raises(DefinitionError, match="one source"):
        build_triggers(project)


def test_on_change_rejects_a_non_identifier() -> None:
    project = compile_models([ModelDef(name="m", sql="SELECT 1 FROM events", schedule={"on_change": "updated at"})])
    with pytest.raises(DefinitionError, match="column"):
        build_triggers(project)


async def test_fresh_enqueues_when_the_timestamp_is_old_and_dedupes_within_the_window(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    engine, store = env
    await engine.execute_sql("CREATE TABLE events (id INTEGER, updated_at TIMESTAMP)")
    await engine.execute_sql("INSERT INTO events VALUES (1, TIMESTAMP '2020-01-01')")
    project = compile_models(
        [
            ModelDef(name="m", sql="SELECT id FROM events", schedule={"fresh": "updated_at 2h"}),
            ModelDef(name="mart", sql="SELECT id FROM m"),
        ]
    )
    trigger = TriggerEngine(build_triggers(project), store, project, engines=engine)
    now = datetime(2026, 1, 1, 12, 0)
    assert await trigger.tick(now) == 1
    assert await trigger.tick(now) == 0  # same window, still stale
    assert await trigger.tick(now + timedelta(hours=3)) == 1  # next window, still stale
    runs = await store.list_runs()
    assert len(runs) == 2
    assert all(set(run["flow_selector"]) == {"m", "mart"} for run in runs)


async def test_fresh_stays_quiet_when_the_timestamp_is_recent(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    engine, store = env
    await engine.execute_sql("CREATE TABLE events (id INTEGER, updated_at TIMESTAMP)")
    await engine.execute_sql("INSERT INTO events VALUES (1, now())")
    project = compile_models([ModelDef(name="m", sql="SELECT id FROM events", schedule={"fresh": "updated_at 2h"})])
    trigger = TriggerEngine(build_triggers(project), store, project, engines=engine)
    assert await trigger.tick(datetime(2026, 1, 1, 12, 0)) == 0


async def test_fresh_treats_an_empty_table_as_stale_and_a_missing_table_as_waiting(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    engine, store = env
    await engine.execute_sql("CREATE TABLE events (updated_at TIMESTAMP)")
    empty = compile_models(
        [ModelDef(name="m", sql="SELECT updated_at FROM events", schedule={"fresh": "updated_at 2h"})]
    )
    trigger = TriggerEngine(build_triggers(empty), store, empty, engines=engine)
    assert await trigger.tick(datetime(2026, 1, 1, 12, 0)) == 1
    missing = compile_models(
        [ModelDef(name="m", sql="SELECT updated_at FROM absent", schedule={"fresh": "updated_at 2h"})]
    )
    waiting = TriggerEngine(build_triggers(missing), store, missing, engines=engine)
    assert await waiting.tick(datetime(2026, 1, 1, 12, 0)) == 0


async def test_fresh_raises_when_the_column_is_not_a_timestamp(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    engine, store = env
    await engine.execute_sql("CREATE TABLE events (updated_at INTEGER)")
    await engine.execute_sql("INSERT INTO events VALUES (10)")
    project = compile_models(
        [ModelDef(name="m", sql="SELECT updated_at FROM events", schedule={"fresh": "updated_at 2h"})]
    )
    trigger = TriggerEngine(build_triggers(project), store, project, engines=engine)
    with pytest.raises(Exception, match="INTEGER"):
        await trigger.tick(datetime(2026, 1, 1, 12, 0))


def test_fresh_mapping_normalises_to_column_and_window() -> None:
    definition = ModelDef(
        name="m",
        sql="SELECT id FROM events",
        schedule={"fresh": {"column": "events.updated_at", "within": "2h"}},  # type: ignore[dict-item]
    )
    assert definition.schedule == {"fresh": "events.updated_at 2h"}
    project = compile_models([definition])
    triggers = build_triggers(project)
    assert len(triggers) == 1
    assert triggers[0].id == "fresh:m"


def test_fresh_rejects_a_bad_window_and_a_non_identifier() -> None:
    bad_window = compile_models([ModelDef(name="m", sql="SELECT 1 FROM events", schedule={"fresh": "updated_at 2x"})])
    with pytest.raises(DefinitionError, match="grain"):
        build_triggers(bad_window)
    bad_column = compile_models([ModelDef(name="m", sql="SELECT 1 FROM events", schedule={"fresh": "updated at 2h"})])
    with pytest.raises(DefinitionError, match="column"):
        build_triggers(bad_column)
    with pytest.raises(DefinitionError, match="within"):
        ModelDef(name="m", sql="SELECT 1", schedule={"fresh": {"column": "updated_at"}})  # type: ignore[dict-item]


def test_watch_pattern_must_be_relative(tmp_path: Path) -> None:
    project = compile_models([ModelDef(name="m", sql="SELECT 1", schedule={"watch": "/tmp/*.csv"})])
    with pytest.raises(DefinitionError, match="relative"):
        build_triggers(project, root=tmp_path)


def test_webhook_names_must_be_unique() -> None:
    project = compile_models(
        [
            ModelDef(name="a", sql="SELECT 1", schedule={"webhook": "landed"}),
            ModelDef(name="b", sql="SELECT 1", schedule={"webhook": "landed"}),
        ]
    )
    with pytest.raises(DefinitionError, match="both"):
        build_triggers(project)


async def test_engine_tick_enqueues_then_dedupes(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    _, store = env
    project = compile_models([ModelDef(name="m", sql="SELECT 1 AS x", schedule={"every": "1h"})])
    engine = TriggerEngine(build_triggers(project), store, project)

    now = datetime(2026, 1, 1, 12, 0)
    assert await engine.tick(now) == 1  # first tick enqueues
    assert await engine.tick(now) == 0  # same tick: last_fired advanced, nothing new
    assert await store.count_pending_runs() == 1


async def test_a_trigger_enqueues_downstream_models(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    _, store = env
    project = compile_models(
        [
            ModelDef(name="raw", sql="SELECT 1 AS id", schedule={"every": "1h"}),
            ModelDef(name="mart", sql="SELECT id FROM raw"),
        ]
    )
    engine = TriggerEngine(build_triggers(project), store, project)
    assert await engine.tick(datetime(2026, 1, 1, 12, 0)) == 1
    runs = await store.list_runs()
    assert set(runs[0]["flow_selector"]) == {"raw", "mart"}
    assert scheduled_closure(project, ["mart"]) == ["mart"]


async def test_worker_drains_and_executes_a_run(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([ModelDef(name="m", sql="SELECT 7 AS x")])
    await store.enqueue_run("k1", ["m"], None, 0)

    processed = await drain(store, project, engine, "prod")
    assert processed == 1
    assert await store.count_pending_runs() == 0

    reader = await engine.fetch(sqlglot.parse_one("SELECT x FROM main.m"))
    assert reader.read_all().to_pylist() == [{"x": 7}]


async def test_a_retry_sees_only_models_that_finished(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    _, store = env
    await store.append_event("model.done", entity="raw", payload={"run": 4})
    await store.append_event("model.failed", entity="mart", payload={"run": 4, "message": "boom"})
    await store.append_event("model.done", entity="other", payload={"run": 9})
    assert await _finished_models(store, 4) == {"raw"}


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


async def test_startup_apply_builds_then_stays_current(tmp_path: Path) -> None:
    project_dir = tmp_path / "proj"
    (project_dir / "models").mkdir(parents=True)
    (project_dir / "interlace.yaml").write_text("name: boot\n")
    (project_dir / "models" / "m.sql").write_text("SELECT 5 AS x\n")
    project = Project.load(project_dir)
    engines = project.open_engines()
    store = await project.open_state()
    state = SimpleNamespace(
        compiled=project.compile(),
        environment="dev",
        project=project,
        engines=engines,
        store=store,
        lock_owner="test",
        connections=project.config.connections,
    )
    try:
        await startup_apply(state)
        reader = await engines.get().fetch(sqlglot.parse_one("SELECT x FROM dev__main.m"))
        assert reader.read_all().to_pylist() == [{"x": 5}]
        await startup_apply(state)
    finally:
        await store.close()
        engines.close()


def test_serve_applies_on_startup_unless_told_not_to() -> None:
    result = runner.invoke(app, ["serve", "--help"])
    assert result.exit_code == 0, result.output
    assert "--apply" in result.output and "--no-apply" in result.output


def test_scheduler_once_builds_a_scheduled_model(tmp_path: Path) -> None:
    project_dir = tmp_path / "proj"
    (project_dir / "models").mkdir(parents=True)
    (project_dir / "interlace.yaml").write_text("name: sched\n")
    (project_dir / "models" / "m.sql").write_text("/*\ninterlace:\n  schedule:\n    every: 1s\n*/\nSELECT 5 AS x")

    result = runner.invoke(app, ["scheduler", "--env", "dev", "--path", str(project_dir), "--once"])
    assert result.exit_code == 0, result.output

    con = duckdb.connect(str(project_dir / ".interlace" / "warehouse.duckdb"))
    try:
        assert con.execute("SELECT x FROM dev__main.m").fetchone() == (5,)
    finally:
        con.close()
