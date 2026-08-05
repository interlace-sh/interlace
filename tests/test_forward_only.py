"""Forward-only application: history-keeping models carry their history across
definition changes via copy-on-write — checks still gate, rollback still works."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from conftest import fetch_rows as _rows

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.state.snapshot import ChangeCategory
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[tuple[DuckDBAdapter, SqliteStateStore]]:
    engine = DuckDBAdapter.in_memory()
    await engine.execute_sql("CREATE SCHEMA raw")
    await engine.execute_sql(
        "CREATE TABLE raw.customers AS SELECT * FROM (VALUES (1,'ada','gold'),(2,'bob','silver')) t(id,name,tier)"
    )
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield engine, store
    await store.close()
    engine.close()


def _dim(sql: str) -> ModelDef:
    return ModelDef(name="dim", sql=sql, strategy="scd", key=("id",))


async def _apply(env: tuple[DuckDBAdapter, SqliteStateStore], model: ModelDef, *, forward_only: bool = False):
    engine, store = env
    compiled = compile_models([model])
    plan = await diff(compiled, "dev", store, forward_only=forward_only)
    return plan, await apply(plan, compiled=compiled, engine=engine, state=store)


async def test_forward_only_preserves_scd2_history(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    v1 = _dim("SELECT id, name, tier FROM raw.customers")
    await _apply(env, v1)
    await engine.execute_sql("UPDATE raw.customers SET tier='gold' WHERE id=2")
    from interlace.plan.run import run_plan  # a normal data-driven run accumulates history

    compiled = compile_models([v1])
    await apply(await run_plan(compiled, "dev", store), compiled=compiled, engine=engine, state=store)
    first_env = await store.get_environment("dev")

    # definition change (a filter — semantic, shape-compatible), applied forward-only
    v2 = _dim("SELECT id, name, tier FROM raw.customers WHERE tier <> 'banned'")
    plan, _ = await _apply(env, v2, forward_only=True)

    change = plan.changes[0]
    assert change.category is ChangeCategory.FORWARD_ONLY
    second_env = await store.get_environment("dev")
    assert second_env["dim"] != first_env["dim"]  # fingerprint advanced

    old = await store.get_snapshot("dim", first_env["dim"])
    new = await store.get_snapshot("dim", second_env["dim"])
    assert new is not None and old is not None
    assert new.physical_table != old.physical_table  # copy-on-write: history moved to a NEW table
    exists = await engine.table_exists(old.physical_table)
    assert exists  # the old table survives as the rollback until gc

    rows = await _rows(engine, "SELECT id, tier, _valid_to IS NULL AS open FROM dev__main.dim ORDER BY id, open")
    assert (2, "silver", False) in [tuple(r.values()) for r in rows]  # bob's closed history survived the copy
    assert (2, "gold", True) in [tuple(r.values()) for r in rows]


async def test_without_flag_history_resets(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await _apply(env, _dim("SELECT id, name, tier FROM raw.customers"))
    first_env = await store.get_environment("dev")

    v2 = _dim("SELECT id, name, tier FROM raw.customers WHERE tier <> 'banned'")
    plan, _ = await _apply(env, v2)  # no flag: snapshot semantics, fresh table

    assert plan.changes[0].category is ChangeCategory.BREAKING
    second_env = await store.get_environment("dev")
    old = await store.get_snapshot("dim", first_env["dim"])
    new = await store.get_snapshot("dim", second_env["dim"])
    assert new is not None and old is not None
    assert new.physical_table != old.physical_table


async def test_full_refresh_models_ignore_the_flag(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    v1 = ModelDef(name="plain", sql="SELECT 1 AS x")
    await _apply(env, v1)
    plan, _ = await _apply(env, ModelDef(name="plain", sql="SELECT 2 AS x"), forward_only=True)

    assert plan.changes[0].category is ChangeCategory.BREAKING  # nothing to preserve: normal rebuild
    assert await _rows(engine, "SELECT x FROM dev__main.plain") == [{"x": 2}]


async def test_forward_only_inherits_interval_ledger(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    from datetime import datetime

    from interlace.state.interval import Interval

    engine, store = env
    v1 = ModelDef(
        name="inc",
        sql="SELECT id, name FROM raw.customers",
        strategy="incremental_by_time",
        time_column="ts",
        interval="1d",
    )
    compiled = compile_models([v1])
    fp1 = compiled.models["inc"].fingerprint
    await store.promote("dev", {"inc": fp1})
    from interlace.plan.differ import snapshot_of

    snap = snapshot_of(compiled.models["inc"], ChangeCategory.BREAKING)
    await store.add_snapshot(snap)
    filled = Interval(datetime(2026, 7, 1), datetime(2026, 7, 2))
    await store.record_interval("inc", fp1, filled)

    v2 = ModelDef(
        name="inc",
        sql="SELECT id, name FROM raw.customers WHERE id > 0",
        strategy="incremental_by_time",
        time_column="ts",
        interval="1d",
    )
    compiled2 = compile_models([v2])
    plan = await diff(compiled2, "dev", store, forward_only=True)

    task = plan.backfills[0]
    assert task.snapshot.change_category is ChangeCategory.FORWARD_ONLY
    assert filled in list(task.snapshot.intervals)  # ledger carried over: no re-backfill of old windows


async def test_forward_only_apply_persists_the_inherited_ledger(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    """Regression: apply used to persist only the freshly-built window, so the next
    catchup saw all history as missing and rewrote it with the new logic — exactly
    what forward-only exists to prevent."""
    from datetime import datetime

    from interlace.plan.run import run_plan
    from interlace.state.interval import Interval

    engine, store = env
    await engine.execute_sql(
        "CREATE TABLE raw.events AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2026-01-01 10:00', 1), (TIMESTAMP '2026-01-02 10:00', 2)) t(ts, id)"
    )

    def inc(sql: str) -> ModelDef:
        return ModelDef(name="inc", sql=sql, strategy="incremental_by_time", time_column="ts", interval="1d")

    v1 = inc("SELECT ts, id FROM raw.events")
    compiled = compile_models([v1])
    plan = await run_plan(compiled, "dev", store, start=datetime(2026, 1, 1), end=datetime(2026, 1, 3))
    await apply(plan, compiled=compiled, engine=engine, state=store)
    history = Interval(datetime(2026, 1, 1), datetime(2026, 1, 3))
    assert (await store.get_intervals("inc", compiled.models["inc"].fingerprint)).covers(history)

    v2 = inc("SELECT ts, id FROM raw.events WHERE id > 0")  # semantic change, shape-compatible
    compiled2 = compile_models([v2])
    await apply(await diff(compiled2, "dev", store, forward_only=True), compiled=compiled2, engine=engine, state=store)

    persisted = await store.get_intervals("inc", compiled2.models["inc"].fingerprint)
    assert persisted.covers(history)  # the inherited windows survived persistence


async def test_forward_only_check_failure_leaves_production_untouched(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    """The audit's HIGH-2 regression: a gated forward-only apply must never mutate
    the table production views are serving."""
    from interlace.checks.spec import parse_checks
    from interlace.exceptions import CheckError

    engine, store = env
    v1 = _dim("SELECT id, name, tier FROM raw.customers")
    compiled = compile_models([v1])
    await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)
    before = await _rows(engine, "SELECT count(*) AS n FROM main.dim")

    v2 = ModelDef(
        name="dim",
        sql="SELECT id, name, 'banned' AS tier FROM raw.customers",  # every tier changes...
        strategy="scd",
        key=("id",),
        checks=parse_checks([{"accepted_values": {"column": "tier", "values": ["gold", "silver"]}}], "dim"),
    )
    compiled2 = compile_models([v2])
    plan = await diff(compiled2, "prod", store, forward_only=True)
    with pytest.raises(CheckError):
        await apply(plan, compiled=compiled2, engine=engine, state=store)

    # production is exactly as it was: same view target, same rows, no half-migration
    assert await _rows(engine, "SELECT count(*) AS n FROM main.dim") == before
    env_map = await store.get_environment("prod")
    assert env_map["dim"] == compiled.models["dim"].fingerprint  # still on v1
