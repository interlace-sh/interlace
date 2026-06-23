"""Ephemeral models: inlined as CTEs, never materialised."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.ir.relation import TableRef
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.plan.resolve import resolve_model_query
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[tuple[DuckDBAdapter, SqliteStateStore]]:
    engine = DuckDBAdapter.in_memory()
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield engine, store
    await store.close()
    engine.close()


async def _fetch(engine: DuckDBAdapter, sql: str) -> list[dict]:
    reader = await engine.fetch(sqlglot.parse_one(sql))
    return reader.read_all().to_pylist()


def test_resolve_inlines_ephemeral_as_cte() -> None:
    project = compile_models(
        [
            ModelDef(name="raw", sql="SELECT 1 AS id, 5 AS val"),
            ModelDef(name="stg", sql="SELECT id, val * 2 AS v FROM raw", materialise="ephemeral"),
            ModelDef(name="final", sql="SELECT id, v FROM stg"),
        ]
    )
    sql = resolve_model_query(project.models["final"], project).sql(dialect="duckdb")
    assert "WITH _eph_stg AS" in sql
    assert "FROM _eph_stg" in sql
    assert project.models["raw"].physical_table.name in sql  # raw resolved to its physical table


def test_chained_ephemeral_inlines_in_dependency_order() -> None:
    project = compile_models(
        [
            ModelDef(name="raw", sql="SELECT 1 AS id"),
            ModelDef(name="a", sql="SELECT id FROM raw", materialise="ephemeral"),
            ModelDef(name="b", sql="SELECT id FROM a", materialise="ephemeral"),
            ModelDef(name="final", sql="SELECT id FROM b"),
        ]
    )
    sql = resolve_model_query(project.models["final"], project).sql(dialect="duckdb")
    assert sql.index("_eph_a AS") < sql.index("_eph_b AS")  # a defined before b
    assert "FROM _eph_b" in sql


async def test_ephemeral_not_built_but_downstream_gets_its_data(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models(
        [
            ModelDef(name="raw", sql="SELECT 1 AS id, 5 AS val"),
            ModelDef(name="stg", sql="SELECT id, val * 2 AS v FROM raw", materialise="ephemeral"),
            ModelDef(name="final", sql="SELECT id, v FROM stg"),
        ]
    )
    result = await apply(await diff(project, "dev", store), compiled=project, engine=engine, state=store)

    assert set(result.built) == {"raw", "final"}  # stg not built
    assert not await engine.table_exists(project.models["stg"].physical_table)  # no physical table
    assert not await engine.table_exists(TableRef(schema="dev__main", name="stg"))  # no env view
    assert await _fetch(engine, "SELECT id, v FROM dev__main.final") == [{"id": 1, "v": 10}]
    assert "stg" in await store.get_environment("dev")  # fingerprint still tracked


async def test_ephemeral_is_not_backfilled(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    _, store = env
    project = compile_models(
        [
            ModelDef(name="stg", sql="SELECT 1 AS x", materialise="ephemeral"),
            ModelDef(name="final", sql="SELECT x FROM stg"),
        ]
    )
    plan = await diff(project, "dev", store)
    assert {task.snapshot.name for task in plan.backfills} == {"final"}
    assert {change.name for change in plan.changes} == {"stg", "final"}  # both still reported
