"""End-to-end incremental: windowed processing + interval ledger."""

from __future__ import annotations

from datetime import datetime

import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import snapshot_of
from interlace.plan.plan import BackfillTask, Plan, ViewSwap, env_view
from interlace.plan.run import run_plan
from interlace.state.interval import Interval
from interlace.state.snapshot import ChangeCategory
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


def d(day: int) -> datetime:
    return datetime(2026, 1, day)


async def _fetch(engine: DuckDBAdapter, sql: str) -> list[dict]:
    reader = await engine.fetch(sqlglot.parse_one(sql))
    return reader.read_all().to_pylist()


def _windowed_plan(model, intervals: list[Interval]) -> Plan:
    snap = snapshot_of(model, ChangeCategory.BREAKING)
    plan = Plan(environment="prod")
    plan.backfills = [BackfillTask(snapshot=snap, interval=iv) for iv in intervals]
    plan.virtual_updates = [ViewSwap(env_view("prod", model.name), model.physical_table)]
    return plan


async def test_incremental_processes_windows_and_fills_the_ledger(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS main")
    await engine.execute_sql(
        "CREATE TABLE main.events AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2026-01-01 10:00', 1), (TIMESTAMP '2026-01-02 10:00', 2), (TIMESTAMP '2026-01-03 10:00', 3)"
        ") v(ts, val)"
    )
    project = compile_models(
        [ModelDef(name="agg", sql="SELECT ts, val FROM main.events", strategy="incremental", time_column="ts")]
    )
    model = project.models["agg"]

    # process days 1 and 2 (not day 3)
    await apply(
        _windowed_plan(model, [Interval(d(1), d(2)), Interval(d(2), d(3))]),
        compiled=project,
        engine=engine,
        state=store,
    )

    rows = await _fetch(engine, "SELECT val FROM main.agg ORDER BY val")
    assert [r["val"] for r in rows] == [1, 2]  # day 3 excluded

    # the ledger records the contiguous filled range [d1, d3)
    intervals = list(await store.get_intervals("agg", model.fingerprint))
    assert intervals == [Interval(d(1), d(3))]


async def test_apply_bootstraps_incremental_from_source_range(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """`interlace apply` on a fresh incremental model derives the initial window
    from the source's time-column range — historical data loads without
    --start/--end, as ONE covering ledger interval."""
    from interlace.plan.differ import diff

    engine, store = env
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS main")
    await engine.execute_sql(
        "CREATE TABLE main.events AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2026-06-01 08:00:00', 1), (TIMESTAMP '2026-06-15 09:00:00', 2), "
        "(TIMESTAMP '2026-06-29 10:00:00', 3)) AS t (ts, val)"
    )
    project = compile_models(
        [ModelDef(name="agg", sql="SELECT ts, val FROM main.events", strategy="incremental", time_column="ts")]
    )

    plan = await diff(project, "prod", store)
    assert len(plan.backfills) == 1
    assert plan.backfills[0].bootstrap and plan.backfills[0].interval is None

    result = await apply(plan, compiled=project, engine=engine, state=store)
    assert result.built == ["agg"]
    rows = await _fetch(engine, "SELECT val FROM main.agg ORDER BY val")
    assert [r["val"] for r in rows] == [1, 2, 3]  # the WHOLE June range, not just the latest day

    model = project.models["agg"]
    ledger = list(await store.get_intervals("agg", model.fingerprint))
    assert len(ledger) == 1  # one covering interval
    assert ledger[0].start.strftime("%Y-%m-%d") == "2026-06-01"
    assert ledger[0].end.strftime("%Y-%m-%d") == "2026-06-30"  # ceiled past the max day


async def test_backfill_none_keeps_latest_window_only(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """backfill: none opts out — a fresh build fills only the most recent grain."""
    from interlace.plan.differ import diff

    engine, store = env
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS main")
    await engine.execute_sql(
        "CREATE TABLE main.events AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2026-06-01 08:00:00', 1), "
        "(date_trunc('day', now()) - INTERVAL '23' HOUR, 2)) AS t (ts, val)"  # yesterday 01:00 — inside the complete window
    )
    project = compile_models(
        [
            ModelDef(
                name="agg",
                sql="SELECT ts, val FROM main.events",
                strategy="incremental",
                time_column="ts",
                backfill="none",
            )
        ]
    )

    plan = await diff(project, "prod", store)
    task = plan.backfills[0]
    assert task.interval is not None and not task.bootstrap
    # the default window is the most recent COMPLETE aligned grain (all of yesterday)
    assert task.interval.start.time().isoformat() == "00:00:00"
    assert task.interval.end - task.interval.start == __import__("datetime").timedelta(days=1)
    await apply(plan, compiled=project, engine=engine, state=store)
    rows = await _fetch(engine, "SELECT val FROM main.agg")
    assert [r["val"] for r in rows] == [2]  # only yesterday's row; June needs an explicit window


async def test_run_plan_expands_window_and_catches_up(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS main")
    await engine.execute_sql(
        "CREATE TABLE main.events AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2026-01-01 10:00', 1), (TIMESTAMP '2026-01-02 10:00', 2), (TIMESTAMP '2026-01-03 10:00', 3)"
        ") v(ts, val)"
    )
    project = compile_models(
        [
            ModelDef(
                name="agg",
                sql="SELECT ts, val FROM main.events",
                strategy="incremental",
                time_column="ts",
                interval="1d",
            )
        ]
    )

    # window of three days -> three missing intervals
    plan = await run_plan(project, "dev", store, start=d(1), end=d(4))
    assert len([task for task in plan.backfills if task.interval is not None]) == 3

    await apply(plan, compiled=project, engine=engine, state=store)
    assert (await _fetch(engine, "SELECT count(*) AS n FROM dev__main.agg")) == [{"n": 3}]

    # re-running the same window is a no-op: every interval is already filled
    caught_up = await run_plan(project, "dev", store, start=d(1), end=d(4))
    assert caught_up.backfills == []


async def test_restate_reprocesses_filled_intervals(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS main")
    await engine.execute_sql(
        "CREATE TABLE main.events AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2026-01-01 10:00', 1), (TIMESTAMP '2026-01-02 10:00', 2)) v(ts, val)"
    )
    project = compile_models(
        [
            ModelDef(
                name="agg",
                sql="SELECT ts, val FROM main.events",
                strategy="incremental",
                time_column="ts",
                interval="1d",
            )
        ]
    )

    # fill the ledger for the window
    await apply(
        await run_plan(project, "dev", store, start=d(1), end=d(3)), compiled=project, engine=engine, state=store
    )
    assert (await run_plan(project, "dev", store, start=d(1), end=d(3))).backfills == []  # caught up

    # restate reprocesses every interval despite being filled
    restated = await run_plan(project, "dev", store, start=d(1), end=d(3), restate=True)
    assert len([task for task in restated.backfills if task.interval is not None]) == 2


async def test_reprocessing_a_window_is_idempotent(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS main")
    await engine.execute_sql(
        "CREATE TABLE main.events AS SELECT * FROM (VALUES (TIMESTAMP '2026-01-01 10:00', 1)) v(ts, val)"
    )
    project = compile_models(
        [ModelDef(name="agg", sql="SELECT ts, val FROM main.events", strategy="incremental", time_column="ts")]
    )
    model = project.models["agg"]
    window = [Interval(d(1), d(2))]

    await apply(_windowed_plan(model, window), compiled=project, engine=engine, state=store)
    await apply(_windowed_plan(model, window), compiled=project, engine=engine, state=store)  # re-run

    rows = await _fetch(engine, "SELECT count(*) AS n FROM main.agg")
    assert rows == [{"n": 1}]  # not duplicated


async def test_unkeyed_window_is_authoritative(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """Without a key the window owns its rows: one that leaves the source is dropped."""
    engine, store = env
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS main")
    await engine.execute_sql(
        "CREATE TABLE main.events AS SELECT * FROM (VALUES "
        "(1, TIMESTAMP '2026-01-01 10:00', 'a'), (2, TIMESTAMP '2026-01-01 11:00', 'b')) v(id, ts, val)"
    )
    project = compile_models(
        [ModelDef(name="agg", sql="SELECT id, ts, val FROM main.events", strategy="incremental", time_column="ts")]
    )
    window = [Interval(d(1), d(2))]
    await apply(_windowed_plan(project.models["agg"], window), compiled=project, engine=engine, state=store)
    assert await _fetch(engine, "SELECT count(*) AS n FROM main.agg") == [{"n": 2}]

    # id 2 leaves the source entirely; reprocess the same window.
    await engine.execute_sql("DELETE FROM main.events WHERE id = 2")
    await apply(_windowed_plan(project.models["agg"], window), compiled=project, engine=engine, state=store)

    rows = await _fetch(engine, "SELECT id FROM main.agg ORDER BY id")
    assert rows == [{"id": 1}], "the window was rewritten, so the departed row is gone"


async def test_keyed_window_only_touches_matching_keys(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """With a key the window only bounds what is read: an unmatched target row survives."""
    engine, store = env
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS main")
    await engine.execute_sql(
        "CREATE TABLE main.events AS SELECT * FROM (VALUES "
        "(1, TIMESTAMP '2026-01-01 10:00', 'a'), (2, TIMESTAMP '2026-01-01 11:00', 'b')) v(id, ts, val)"
    )
    project = compile_models(
        [
            ModelDef(
                name="agg",
                sql="SELECT id, ts, val FROM main.events",
                strategy="incremental",
                time_column="ts",
                key=("id",),
            )
        ]
    )
    window = [Interval(d(1), d(2))]
    await apply(_windowed_plan(project.models["agg"], window), compiled=project, engine=engine, state=store)
    assert await _fetch(engine, "SELECT count(*) AS n FROM main.agg") == [{"n": 2}]

    # id 2 leaves the source; id 1 changes. Same window.
    await engine.execute_sql("DELETE FROM main.events WHERE id = 2")
    await engine.execute_sql("UPDATE main.events SET val = 'a2' WHERE id = 1")
    await apply(_windowed_plan(project.models["agg"], window), compiled=project, engine=engine, state=store)

    rows = await _fetch(engine, "SELECT id, val FROM main.agg ORDER BY id")
    assert rows == [
        {"id": 1, "val": "a2"},
        {"id": 2, "val": "b"},
    ], "the upsert updated id 1 and left id 2 alone — the opposite of the unkeyed case"


def test_the_old_strategy_name_explains_the_rename() -> None:
    from interlace.exceptions import PlanError
    from interlace.strategies import resolve_strategy

    with pytest.raises(PlanError, match="renamed to incremental"):
        resolve_strategy("virtual", "incremental_by_time", time_column="ts")
