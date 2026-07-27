"""End-to-end incremental_by_time: windowed processing + interval ledger."""

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
        [ModelDef(name="agg", sql="SELECT ts, val FROM main.events", strategy="incremental_by_time", time_column="ts")]
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


async def test_apply_schedules_latest_window_for_incremental(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """`interlace apply` on a project with an incremental model must work: diff
    schedules the latest grain interval (run --start/--end covers history)."""
    from interlace.plan.differ import diff

    engine, store = env
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS main")
    await engine.execute_sql("CREATE TABLE main.events AS SELECT now() AS ts, 1 AS val")
    project = compile_models(
        [ModelDef(name="agg", sql="SELECT ts, val FROM main.events", strategy="incremental_by_time", time_column="ts")]
    )

    plan = await diff(project, "prod", store)
    assert len(plan.backfills) == 1
    assert plan.backfills[0].interval is not None  # windowed, not the unbuildable bare task

    result = await apply(plan, compiled=project, engine=engine, state=store)
    assert result.built == ["agg"]
    rows = await _fetch(engine, "SELECT val FROM main.agg")
    assert [r["val"] for r in rows] == [1]  # now() falls inside the latest-day window


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
                strategy="incremental_by_time",
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
                strategy="incremental_by_time",
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
        [ModelDef(name="agg", sql="SELECT ts, val FROM main.events", strategy="incremental_by_time", time_column="ts")]
    )
    model = project.models["agg"]
    window = [Interval(d(1), d(2))]

    await apply(_windowed_plan(model, window), compiled=project, engine=engine, state=store)
    await apply(_windowed_plan(model, window), compiled=project, engine=engine, state=store)  # re-run

    rows = await _fetch(engine, "SELECT count(*) AS n FROM main.agg")
    assert rows == [{"n": 1}]  # not duplicated
