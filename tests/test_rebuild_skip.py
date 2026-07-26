"""Indirect non-breaking rebuild-skip: a downstream model whose output is
provably identical reuses its previous physical table instead of rebuilding.

The invariant under test: an indirectly-changed model's SQL is unchanged and was
previously valid, so it cannot reference newly-added upstream columns — only a
projection ``*`` (or a Python model, which sees whole tables) inherits them.
"""

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
from interlace.state.snapshot import ChangeCategory
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[tuple[DuckDBAdapter, SqliteStateStore]]:
    engine = DuckDBAdapter.in_memory()
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield engine, store
    await store.close()
    engine.close()


def sql_model(name: str, sql: str, **kwargs: object) -> ModelDef:
    return ModelDef(name=name, sql=sql, **kwargs)  # type: ignore[arg-type]


async def _rows(engine: DuckDBAdapter, sql: str) -> list[dict]:
    reader = await engine.fetch(sqlglot.parse_one(sql))
    return reader.read_all().to_pylist()


async def _apply(env: tuple[DuckDBAdapter, SqliteStateStore], models: list[ModelDef], environment: str = "prod"):
    engine, store = env
    compiled = compile_models(models)
    plan = await diff(compiled, environment, store)
    return plan, await apply(plan, compiled=compiled, engine=engine, state=store)


async def test_clean_downstream_reuses_previous_table(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    v1 = [sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT x FROM up")]
    await _apply(env, v1)
    first = await store.get_environment("prod")

    v2 = [sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT x FROM up")]
    plan, result = await _apply(env, v2)

    assert result.built == ["up"]  # only the directly-changed model rebuilt
    assert result.reused == ["down"]
    second = await store.get_environment("prod")
    assert second["down"] != first["down"]  # fingerprint still advanced

    # the new snapshot points at the ORIGINAL physical table
    old = await store.get_snapshot("down", first["down"])
    new = await store.get_snapshot("down", second["down"])
    assert new is not None and old is not None
    assert new.physical_table == old.physical_table
    assert await _rows(engine, "SELECT x FROM main.down") == [{"x": 1}]  # env view still resolves


async def test_star_downstream_rebuilds_and_inherits_columns(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    await _apply(env, [sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT * FROM up")])
    _, result = await _apply(env, [sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT * FROM up")])

    assert set(result.built) == {"up", "down"}  # star inherits the new column -> rebuild
    assert result.reused == []
    assert await _rows(engine, "SELECT x, y FROM main.down") == [{"x": 1, "y": 2}]


async def test_where_change_is_semantic_and_rebuilds_downstream(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """A filter change keeps projections identical but changes data — never skippable."""
    engine, _ = env
    src = "SELECT * FROM (VALUES (1), (2), (3)) AS t (x)"
    await _apply(env, [sql_model("up", f"SELECT x FROM ({src}) q"), sql_model("down", "SELECT x FROM up")])

    v2 = [sql_model("up", f"SELECT x FROM ({src}) q WHERE x > 1"), sql_model("down", "SELECT x FROM up")]
    plan, result = await _apply(env, v2)

    by_name = {c.name: c for c in plan.changes}
    assert by_name["up"].category is ChangeCategory.BREAKING
    assert set(result.built) == {"up", "down"}
    assert result.reused == []
    assert len(await _rows(engine, "SELECT x FROM main.down")) == 2  # downstream sees filtered data


async def test_skip_propagates_down_a_clean_chain(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    v1 = [
        sql_model("a", "SELECT 1 AS x"),
        sql_model("b", "SELECT x FROM a"),
        sql_model("c", "SELECT x FROM b"),
    ]
    await _apply(env, v1)
    v2 = [
        sql_model("a", "SELECT 1 AS x, 2 AS y"),
        sql_model("b", "SELECT x FROM a"),
        sql_model("c", "SELECT x FROM b"),
    ]
    _, result = await _apply(env, v2)

    assert result.built == ["a"]
    assert set(result.reused) == {"b", "c"}  # b is clean, so c's inputs are identical too


async def test_count_star_is_not_a_projection_star(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    v1 = [sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT count(*) AS n FROM up")]
    await _apply(env, v1)
    v2 = [sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT count(*) AS n FROM up")]
    _, result = await _apply(env, v2)

    assert result.built == ["up"]
    assert result.reused == ["down"]  # count(*) counts rows; additive columns cannot change it


async def test_python_downstream_always_rebuilds(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    import pyarrow as pa

    def snap(up) -> pa.Table:  # a Python model sees every upstream column
        return up.table()

    v1 = [sql_model("up", "SELECT 1 AS x"), ModelDef(name="snap", fn=snap, depends_on=("up",))]
    await _apply(env, v1)
    v2 = [sql_model("up", "SELECT 1 AS x, 2 AS y"), ModelDef(name="snap", fn=snap, depends_on=("up",))]
    _, result = await _apply(env, v2)

    assert set(result.built) == {"up", "snap"}
    assert result.reused == []


async def test_rebuilt_model_resolves_reused_upstream_table(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """c reads clean-reused b and semantic d: c rebuilds and must find b's data
    at its OLD physical table (the fingerprint-derived name was never built)."""
    engine, _ = env
    v1 = [
        sql_model("a", "SELECT 1 AS x"),
        sql_model("b", "SELECT x FROM a"),
        sql_model("d", "SELECT 10 AS w"),
        sql_model("c", "SELECT b.x, d.w FROM b, d"),
    ]
    await _apply(env, v1)
    v2 = [
        sql_model("a", "SELECT 1 AS x, 2 AS y"),  # additive -> b clean-reused
        sql_model("b", "SELECT x FROM a"),
        sql_model("d", "SELECT 99 AS w"),  # semantic -> c rebuilds
        sql_model("c", "SELECT b.x, d.w FROM b, d"),
    ]
    _, result = await _apply(env, v2)

    assert set(result.built) == {"a", "d", "c"}
    assert result.reused == ["b"]
    assert await _rows(engine, "SELECT x, w FROM main.c") == [{"x": 1, "w": 99}]


async def test_reuse_survives_plan_render_fields(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    await _apply(env, [sql_model("up", "SELECT 1 AS x"), sql_model("down", "SELECT x FROM up")])
    engine, store = env
    compiled = compile_models([sql_model("up", "SELECT 1 AS x, 2 AS y"), sql_model("down", "SELECT x FROM up")])
    plan = await diff(compiled, "prod", store)

    up = next(c for c in plan.changes if c.name == "up")
    assert up.impacted_columns == ("y",)  # additive columns surfaced for diff display
    assert {s.name for s in plan.reuses} == {"down"}
    assert {t.snapshot.name for t in plan.backfills} == {"up"}
