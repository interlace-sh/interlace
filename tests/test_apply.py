"""End-to-end plan -> apply against a real DuckDB engine + SQLite state."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.plan.plan import ChangeType
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


def sql_model(name: str, sql: str, **kwargs: object) -> ModelDef:
    return ModelDef(name=name, sql=sql, **kwargs)  # type: ignore[arg-type]


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[tuple[DuckDBAdapter, SqliteStateStore]]:
    engine = DuckDBAdapter.in_memory()
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield engine, store
    await store.close()
    engine.close()


async def _rows(engine: DuckDBAdapter, sql: str) -> list[dict]:
    reader = await engine.fetch(sqlglot.parse_one(sql))
    return reader.read_all().to_pylist()


async def test_apply_builds_dependency_chain_and_env_views(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models(
        [sql_model("a", "SELECT 1 AS id, 10 AS v"), sql_model("b", "SELECT id, v * 2 AS v2 FROM a")]
    )

    result = await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)

    assert set(result.built) == {"a", "b"}
    # the downstream model read through to the upstream's physical table and the env view resolves
    assert await _rows(engine, "SELECT id, v2 FROM prod__main.b") == [{"id": 1, "v2": 20}]


async def test_re_apply_is_a_no_op(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    models = [sql_model("a", "SELECT 1 AS x")]
    await apply(
        await diff(compile_models(models), "prod", store), compiled=compile_models(models), engine=engine, state=store
    )

    plan = await diff(compile_models(models), "prod", store)
    assert plan.is_empty


async def test_view_materialisation(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([sql_model("answer", "SELECT 42 AS n", materialise="view")])
    await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)

    assert await _rows(engine, "SELECT n FROM prod__main.answer") == [{"n": 42}]


async def test_modify_then_reapply_rebuilds_and_repoints(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    v1 = compile_models([sql_model("a", "SELECT 1 AS x")])
    await apply(await diff(v1, "prod", store), compiled=v1, engine=engine, state=store)

    v2 = compile_models([sql_model("a", "SELECT 2 AS x")])
    plan = await diff(v2, "prod", store)
    assert plan.changes[0].change_type is ChangeType.MODIFIED
    await apply(plan, compiled=v2, engine=engine, state=store)

    assert await _rows(engine, "SELECT x FROM prod__main.a") == [{"x": 2}]
