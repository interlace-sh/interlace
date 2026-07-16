"""Incremental Python extraction: keyed strategies, `cursor`/`this` params, full_merge."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import DefinitionError
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.plan.run import run_plan
from interlace.runtime.handles import RelationHandle
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit

Env = tuple[DuckDBAdapter, SqliteStateStore]


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[Env]:
    engine = DuckDBAdapter.in_memory()
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield engine, store
    await store.close()
    engine.close()


async def _rows(engine: DuckDBAdapter, sql: str) -> list[dict]:
    reader = await engine.fetch(sqlglot.parse_one(sql))
    return reader.read_all().to_pylist()


async def _initial(env: Env, models: list[ModelDef]) -> Any:
    engine, store = env
    compiled = compile_models(models)
    await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)
    return compiled


async def _rerun(env: Env, compiled: Any, name: str) -> None:
    engine, store = env
    plan = await run_plan(compiled, "dev", store, select={name})
    await apply(plan, compiled=compiled, engine=engine, state=store)


# --- full_merge (SQL) ----------------------------------------------------------


async def test_full_merge_applies_only_the_diff(env: Env) -> None:
    engine, _ = env
    await engine.execute_sql("CREATE TABLE src_data AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, val)")
    merged = ModelDef(name="merged", sql="SELECT * FROM src_data", strategy="full_merge", key=("id",))
    compiled = await _initial(env, [merged])
    assert await _rows(engine, "SELECT * FROM dev__main.merged ORDER BY id") == [
        {"id": 1, "val": "a"},
        {"id": 2, "val": "b"},
        {"id": 3, "val": "c"},
    ]

    await engine.execute_sql("UPDATE src_data SET val = 'B' WHERE id = 2")  # changed
    await engine.execute_sql("DELETE FROM src_data WHERE id = 3")  # vanished upstream
    await engine.execute_sql("INSERT INTO src_data VALUES (4, 'd')")  # new
    await _rerun(env, compiled, "merged")
    assert await _rows(engine, "SELECT * FROM dev__main.merged ORDER BY id") == [
        {"id": 1, "val": "a"},
        {"id": 2, "val": "B"},
        {"id": 4, "val": "d"},
    ]


async def test_full_merge_rerun_over_identical_data_is_a_noop(env: Env) -> None:
    engine, _ = env
    await engine.execute_sql("CREATE TABLE src_data AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, val)")
    merged = ModelDef(name="merged", sql="SELECT * FROM src_data", strategy="full_merge", key=("id",))
    compiled = await _initial(env, [merged])
    physical = compiled.models["merged"].physical_table
    probe = f"SELECT id, val, rowid FROM {physical.schema}.{physical.name} ORDER BY id"
    before = await _rows(engine, probe)
    await _rerun(env, compiled, "merged")
    after = await _rows(engine, probe)
    assert before == after  # unchanged rows were not rewritten


# --- Python models with keyed strategies ----------------------------------------


async def test_python_merge_by_key_accumulates_across_runs(env: Env) -> None:
    engine, _ = env
    batches = iter(
        [
            pa.table({"id": [1, 2], "val": ["a", "b"]}),
            pa.table({"id": [2, 3], "val": ["B", "c"]}),
        ]
    )

    def extract() -> pa.Table:
        return next(batches)

    events = ModelDef(name="events", fn=extract, strategy="merge_by_key", key=("id",))
    compiled = await _initial(env, [events])
    await _rerun(env, compiled, "events")
    assert await _rows(engine, "SELECT * FROM dev__main.events ORDER BY id") == [
        {"id": 1, "val": "a"},
        {"id": 2, "val": "B"},
        {"id": 3, "val": "c"},
    ]


async def test_python_full_merge_deletes_vanished_keys(env: Env) -> None:
    engine, _ = env
    batches = iter(
        [
            pa.table({"id": [1, 2], "val": ["a", "b"]}),
            pa.table({"id": [2], "val": ["b"]}),
        ]
    )
    listing = ModelDef(name="listing", fn=lambda: next(batches), strategy="full_merge", key=("id",))
    compiled = await _initial(env, [listing])
    await _rerun(env, compiled, "listing")
    assert await _rows(engine, "SELECT * FROM dev__main.listing ORDER BY id") == [{"id": 2, "val": "b"}]


async def test_python_merge_evolves_additive_columns(env: Env) -> None:
    engine, _ = env
    batches = iter(
        [
            pa.table({"id": [1], "val": ["a"]}),
            pa.table({"id": [2], "val": ["b"], "extra": [42]}),  # API grew a field
        ]
    )
    events = ModelDef(name="events", fn=lambda: next(batches), strategy="merge_by_key", key=("id",))
    compiled = await _initial(env, [events])
    await _rerun(env, compiled, "events")
    assert await _rows(engine, "SELECT * FROM dev__main.events ORDER BY id") == [
        {"id": 1, "val": "a", "extra": None},
        {"id": 2, "val": "b", "extra": 42},
    ]


async def test_python_merge_null_fills_vanished_columns(env: Env) -> None:
    engine, _ = env
    batches = iter(
        [
            pa.table({"id": [1], "val": ["a"], "gone": ["x"]}),
            pa.table({"id": [2], "val": ["b"]}),  # API dropped a field
        ]
    )
    events = ModelDef(name="events", fn=lambda: next(batches), strategy="merge_by_key", key=("id",))
    compiled = await _initial(env, [events])
    await _rerun(env, compiled, "events")
    assert await _rows(engine, "SELECT * FROM dev__main.events ORDER BY id") == [
        {"id": 1, "val": "a", "gone": "x"},
        {"id": 2, "val": "b", "gone": None},
    ]


# --- cursor / this --------------------------------------------------------------


async def test_cursor_is_none_then_resumes_from_the_warehouse_max(env: Env) -> None:
    engine, _ = env
    seen: list[Any] = []

    def extract(cursor: int | None) -> pa.Table:
        seen.append(cursor)
        if cursor is None:
            return pa.table({"id": [1, 2], "ts": [10, 20]})
        return pa.table({"id": [3], "ts": [cursor + 10]})

    events = ModelDef(name="events", fn=extract, strategy="merge_by_key", key=("id",), cursor="ts")
    compiled = await _initial(env, [events])
    await _rerun(env, compiled, "events")
    assert seen == [None, 20]
    assert await _rows(engine, "SELECT * FROM dev__main.events ORDER BY id") == [
        {"id": 1, "ts": 10},
        {"id": 2, "ts": 20},
        {"id": 3, "ts": 30},
    ]


async def test_cursor_param_requires_declaration(env: Env) -> None:
    def extract(cursor: Any) -> pa.Table:
        return pa.table({"id": [1]})

    with pytest.raises(DefinitionError, match="cursor"):
        await _initial(env, [ModelDef(name="events", fn=extract, strategy="merge_by_key", key=("id",))])


async def test_this_exposes_the_previous_materialisation(env: Env) -> None:
    engine, _ = env

    def extract(this: RelationHandle | None) -> pa.Table:
        if this is None:
            return pa.table({"id": [1]})
        done = this.table().column("id").to_pylist()
        return pa.table({"id": [max(done) + 1]})

    backfill = ModelDef(name="backfill", fn=extract, strategy="merge_by_key", key=("id",))
    compiled = await _initial(env, [backfill])
    await _rerun(env, compiled, "backfill")
    await _rerun(env, compiled, "backfill")
    assert await _rows(engine, "SELECT * FROM dev__main.backfill ORDER BY id") == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]


async def test_cursor_works_with_full_strategy_too(env: Env) -> None:
    engine, _ = env
    seen: list[Any] = []

    def extract(cursor: int | None) -> pa.Table:
        seen.append(cursor)
        return pa.table({"id": [1], "ts": [99]})

    events = ModelDef(name="events", fn=extract, cursor="ts")  # strategy=full
    compiled = await _initial(env, [events])
    await _rerun(env, compiled, "events")
    assert seen == [None, 99]
    assert await _rows(engine, "SELECT * FROM dev__main.events") == [{"id": 1, "ts": 99}]
