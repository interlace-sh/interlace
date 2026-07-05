"""Snapshot GC: unreferenced rows go, shared physical tables survive reuse."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.state.janitor import gc
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit

NONE = timedelta(0)


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[tuple[DuckDBAdapter, SqliteStateStore]]:
    engine = DuckDBAdapter.in_memory()
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield engine, store
    await store.close()
    engine.close()


def sql_model(name: str, sql: str) -> ModelDef:
    return ModelDef(name=name, sql=sql)


async def _apply(env: tuple[DuckDBAdapter, SqliteStateStore], models: list[ModelDef], environment: str = "prod"):
    engine, store = env
    compiled = compile_models(models)
    return await apply(await diff(compiled, environment, store), compiled=compiled, engine=engine, state=store)


async def _rows(engine: DuckDBAdapter, sql: str) -> list[dict]:
    return (await engine.fetch(sqlglot.parse_one(sql))).read_all().to_pylist()


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
    assert await _rows(engine, "SELECT x FROM prod__main.a") == [{"x": 2}]  # env untouched


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
    assert await _rows(engine, "SELECT x FROM prod__main.down") == [{"x": 1}]


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
    assert await _rows(engine, "SELECT x FROM prod__main.a") == [{"x": 2}]
