"""Snapshot GC: unreferenced rows go, shared physical tables survive reuse."""

from __future__ import annotations

from datetime import timedelta

import pytest
from conftest import fetch_rows as _rows

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.state.janitor import gc
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit

NONE = timedelta(0)


def sql_model(name: str, sql: str) -> ModelDef:
    return ModelDef(name=name, sql=sql)


async def _apply(env: tuple[DuckDBAdapter, SqliteStateStore], models: list[ModelDef], environment: str = "prod"):
    engine, store = env
    compiled = compile_models(models)
    return await apply(await diff(compiled, environment, store), compiled=compiled, engine=engine, state=store)


async def _tables(engine: DuckDBAdapter, like: str) -> list[str]:
    rows = await _rows(
        engine,
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = 'interlace__main' AND table_name LIKE '{like}' ORDER BY table_name",
    )
    return [r["table_name"] for r in rows]


async def test_gc_removes_superseded_snapshot_and_table(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await _apply(env, [sql_model("a", "SELECT 1 AS x")])
    await _apply(env, [sql_model("a", "SELECT 2 AS x")])  # breaking: new fingerprint + table
    assert len(await _tables(engine, "a__%")) == 2

    result = await gc(store, engine, grace=NONE)

    assert len(result.removed_snapshots) == 1
    assert len(result.dropped_tables) == 1
    assert len(await _tables(engine, "a__%")) == 1  # only the promoted table remains
    assert await _rows(engine, "SELECT x FROM main.a") == [{"x": 2}]  # env untouched


async def test_gc_reclaims_a_view_materialised_snapshot(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    # Regression: a `materialise: view` model's physical snapshot is a VIEW, not a table.
    # gc must DROP VIEW it — a blind DROP TABLE raises (CatalogException) and reclaims nothing.
    engine, store = env
    await _apply(env, [ModelDef(name="v", sql="SELECT 1 AS x", materialise="view")])
    await _apply(env, [ModelDef(name="v", sql="SELECT 2 AS x", materialise="view")])  # new fp → old view is garbage
    assert len(await _tables(engine, "v__%")) == 2

    result = await gc(store, engine, grace=NONE)

    assert len(result.removed_snapshots) == 1
    assert len(result.dropped_tables) == 1  # the superseded VIEW was dropped, no DROP TABLE error
    assert len(await _tables(engine, "v__%")) == 1
    assert await _rows(engine, "SELECT x FROM main.v") == [{"x": 2}]  # env view untouched


async def test_gc_keeps_tables_shared_by_reuse(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """The rebuild-skip case: down@v2 reuses down@v1's physical table. GC must
    delete the v1 row but keep the shared table."""
    engine, store = env
    await _apply(env, [sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT x FROM up")])
    result = await _apply(env, [sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT x FROM up")])
    assert result.reused == ["down"]

    outcome = await gc(store, engine, grace=NONE)

    removed_models = {name for name, _ in outcome.removed_snapshots}
    assert removed_models == {"up", "down"}  # both v1 rows are unreferenced now
    down_tables = await _tables(engine, "down__%")
    assert len(down_tables) == 1  # the shared table survived the row deletion
    assert await _rows(engine, "SELECT x FROM main.down") == [{"x": 1}]


async def test_gc_grace_protects_recent_snapshots(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await _apply(env, [sql_model("a", "SELECT 1 AS x")])
    await _apply(env, [sql_model("a", "SELECT 2 AS x")])

    result = await gc(store, engine, grace=timedelta(hours=1))
    assert result.removed_snapshots == []  # superseded but younger than grace
    assert len(await _tables(engine, "a__%")) == 2


async def test_gc_dry_run_touches_nothing(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await _apply(env, [sql_model("a", "SELECT 1 AS x")])
    await _apply(env, [sql_model("a", "SELECT 2 AS x")])

    result = await gc(store, engine, grace=NONE, dry_run=True)
    assert len(result.removed_snapshots) == 1 and len(result.dropped_tables) == 1
    assert len(await _tables(engine, "a__%")) == 2  # nothing dropped
    assert len(await store.list_snapshot_rows()) == 2  # nothing deleted


async def test_gc_respects_every_environment(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await _apply(env, [sql_model("a", "SELECT 1 AS x")], environment="staging")  # v1 stays live in staging
    await _apply(env, [sql_model("a", "SELECT 2 AS x")], environment="prod")

    result = await gc(store, engine, grace=NONE)
    assert result.removed_snapshots == []  # both fingerprints referenced somewhere
    assert await _rows(engine, "SELECT x FROM staging__main.a") == [{"x": 1}]
    assert await _rows(engine, "SELECT x FROM main.a") == [{"x": 2}]


async def test_gc_sweeps_transfer_staging(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await _apply(env, [sql_model("a", "SELECT 1 AS x")])
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS interlace__xfer")
    await engine.execute_sql("CREATE TABLE interlace__xfer.leftover AS SELECT 1 AS x")

    dry = await gc(store, engine, grace=NONE, dry_run=True)
    assert dry.swept_staging == ["default:interlace__xfer.leftover"]
    assert (await _rows(engine, "SELECT count(*) AS n FROM interlace__xfer.leftover")) == [{"n": 1}]  # untouched

    result = await gc(store, engine, grace=NONE)
    assert result.swept_staging == ["default:interlace__xfer.leftover"]
    tables = await _tables(engine, "leftover")
    assert tables == []  # scratch swept; the next apply that needs it re-stages


async def test_drop_environment_releases_snapshots(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    from interlace.state.janitor import drop_environment

    engine, store = env
    await _apply(env, [sql_model("a", "SELECT 1 AS x")], environment="dev")
    assert await _rows(engine, "SELECT x FROM dev__main.a") == [{"x": 1}]

    dropped = await drop_environment(store, engine, environment="dev")

    assert dropped == ["default:dev__main.a"]
    assert await store.get_environment("dev") == {}  # promotion rows gone
    schemas = await _rows(
        engine, "SELECT count(*) AS n FROM information_schema.schemata WHERE schema_name = 'dev__main'"
    )
    assert schemas == [{"n": 0}]  # the sandbox schema itself is gone

    # its snapshots are unreferenced now: gc reclaims the physical table
    result = await gc(store, engine, grace=NONE)
    assert len(result.removed_snapshots) == 1
    assert len(await _tables(engine, "a__%")) == 0


async def test_drop_production_keeps_natural_schemas(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    from interlace.state.janitor import drop_environment

    engine, store = env
    await engine.execute_sql("CREATE TABLE IF NOT EXISTS main.user_owned AS SELECT 1 AS keep")
    await _apply(env, [sql_model("a", "SELECT 1 AS x")])  # prod: main.a view

    dropped = await drop_environment(store, engine, environment="prod")

    assert dropped == ["default:main.a"]
    # only the view went; the natural schema and user tables survive
    assert await _rows(engine, "SELECT keep FROM main.user_owned") == [{"keep": 1}]
