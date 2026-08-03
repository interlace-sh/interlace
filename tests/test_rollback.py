"""Rollback: repoint an environment's views at an earlier promotion generation.
Nothing rebuilds — the views move; apply returns to the latest state."""

from __future__ import annotations

import pytest
from conftest import fetch_rows as _rows

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import PlanError
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.state.janitor import rollback_environment
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


async def _apply(env: tuple[DuckDBAdapter, SqliteStateStore], models: list[ModelDef]) -> None:
    engine, store = env
    compiled = compile_models(models)
    await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)


async def test_rollback_repoints_views_without_rebuilding(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    v1 = [ModelDef(name="m", sql="SELECT 1 AS x")]
    await _apply(env, v1)
    v2 = [ModelDef(name="m", sql="SELECT 2 AS x")]
    await _apply(env, v2)
    assert await _rows(engine, "SELECT x FROM main.m") == [{"x": 2}]

    compiled = compile_models(v2)
    result = await rollback_environment(store, compiled, engine=engine, environment="prod")

    assert result["generation"] == 1 and result["repointed"] == ["m"]
    assert await _rows(engine, "SELECT x FROM main.m") == [{"x": 1}]  # the view moved back
    # promotion map matches generation 1 again
    assert (await store.get_environment("prod"))["m"] != compiled.models["m"].fingerprint
    # applying again returns to the latest state (plan sees drift)
    await _apply(env, v2)
    assert await _rows(engine, "SELECT x FROM main.m") == [{"x": 2}]


async def test_rollback_drops_views_for_models_that_did_not_exist_then(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    engine, store = env
    await _apply(env, [ModelDef(name="a", sql="SELECT 1 AS x")])
    v2 = [ModelDef(name="a", sql="SELECT 1 AS x"), ModelDef(name="late", sql="SELECT 9 AS y")]
    await _apply(env, v2)
    assert await _rows(engine, "SELECT y FROM main.late") == [{"y": 9}]

    result = await rollback_environment(store, compile_models(v2), engine=engine, environment="prod")

    assert result["removed_views"] == ["late"]
    import duckdb

    with pytest.raises(duckdb.CatalogException):
        await _rows(engine, "SELECT y FROM main.late")
    assert "late" not in await store.get_environment("prod")


async def test_rollback_targets_and_history(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    for value in (1, 2, 3):
        await _apply(env, [ModelDef(name="m", sql=f"SELECT {value} AS x")])
    generations = await store.list_generations("prod")
    assert [g["generation"] for g in generations] == [3, 2, 1]

    compiled = compile_models([ModelDef(name="m", sql="SELECT 3 AS x")])
    result = await rollback_environment(store, compiled, engine=engine, environment="prod", to_generation=1)
    assert result["generation"] == 1
    assert await _rows(engine, "SELECT x FROM main.m") == [{"x": 1}]

    with pytest.raises(PlanError, match="valid targets"):
        await rollback_environment(store, compiled, engine=engine, environment="prod", to_generation=99)


async def test_rollback_with_single_generation_refuses(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await _apply(env, [ModelDef(name="m", sql="SELECT 1 AS x")])
    with pytest.raises(PlanError, match="valid targets"):
        await rollback_environment(store, None, engine=engine, environment="prod")
