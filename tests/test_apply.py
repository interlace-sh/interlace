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


async def test_merge_by_key_upserts_across_runs(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    # Drives the strategy + atomic execute_all directly (as a scheduled re-run would),
    # since the differ only re-runs a model when its definition changes.
    from interlace.engines.base import EngineCaps
    from interlace.ir.relation import EngineRef, SqlRelation, TableRef
    from interlace.ir.schema import empty_schema
    from interlace.strategies import MergeByKey

    engine, _ = env
    target = TableRef(schema="main", name="dim")
    strategy = MergeByKey(("id",))
    caps = EngineCaps(supports_create_or_replace=True)

    def relation(sql: str) -> SqlRelation:
        return SqlRelation(ast=sqlglot.parse_one(sql), engine=EngineRef("duckdb", "duckdb"), schema=empty_schema())

    await engine.execute_all(
        strategy.plan_statements(relation("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v(id, name)"), target, caps)
    )
    assert sorted(await _rows(engine, "SELECT id, name FROM main.dim"), key=lambda r: r["id"]) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]

    await engine.execute_all(
        strategy.plan_statements(relation("SELECT * FROM (VALUES (2, 'B'), (3, 'c')) v(id, name)"), target, caps)
    )
    assert sorted(await _rows(engine, "SELECT id, name FROM main.dim"), key=lambda r: r["id"]) == [
        {"id": 1, "name": "a"},  # untouched
        {"id": 2, "name": "B"},  # updated
        {"id": 3, "name": "c"},  # inserted
    ]


async def test_apply_merge_model_first_build(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models(
        [sql_model("dim", "SELECT * FROM (VALUES (1, 'a')) v(id, name)", strategy="merge_by_key", key=("id",))]
    )
    await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
    assert await _rows(engine, "SELECT id, name FROM prod__main.dim") == [{"id": 1, "name": "a"}]


async def test_apply_passes_a_satisfied_contract(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([sql_model("c", "SELECT 1 AS id, 'x' AS name", columns={"id": None, "name": None})])
    result = await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
    assert result.built == ["c"]


async def test_apply_blocks_on_contract_drift(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    from interlace.exceptions import SchemaError

    engine, store = env
    # contract demands a column the query does not produce
    project = compile_models([sql_model("c", "SELECT 1 AS id", columns={"id": None, "missing": None})])
    with pytest.raises(SchemaError):
        await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)

    # promotion did not happen: the model is still pending in a fresh plan
    assert not (await diff(project, "prod", store)).is_empty


async def test_modify_then_reapply_rebuilds_and_repoints(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    v1 = compile_models([sql_model("a", "SELECT 1 AS x")])
    await apply(await diff(v1, "prod", store), compiled=v1, engine=engine, state=store)

    v2 = compile_models([sql_model("a", "SELECT 2 AS x")])
    plan = await diff(v2, "prod", store)
    assert plan.changes[0].change_type is ChangeType.MODIFIED
    await apply(plan, compiled=v2, engine=engine, state=store)

    assert await _rows(engine, "SELECT x FROM prod__main.a") == [{"x": 2}]
