"""Source-schema drift on keyed Python models: the model is unchanged (same
fingerprint) but the upstream API's payload gains a column or changes a field's
type between pulls. New columns are added to the target; numeric widening
promotes the column in place (DuckLake allows exactly those); anything else is
cast to the target's type — loud on genuinely unconvertible values."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.plan.run import run_plan
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


def _model(batches: list[pa.Table]) -> ModelDef:
    feed = iter(batches)

    def extract() -> pa.Table:
        return next(feed)

    return ModelDef(name="events", fn=extract, strategy="merge_by_key", key=("id",))


async def _run_twice(env: Env, batches: list[pa.Table]) -> Any:
    engine, store = env
    compiled = compile_models([_model(batches)])
    await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)
    plan = await run_plan(compiled, "dev", store, select={"events"})
    await apply(plan, compiled=compiled, engine=engine, state=store)
    return compiled


async def test_new_column_is_added(env: Env) -> None:
    engine, _ = env
    await _run_twice(
        env,
        [
            pa.table({"id": ["a"], "v": [1]}),
            pa.table({"id": ["b"], "v": [2], "extra": ["new-field"]}),
        ],
    )
    rows = await _rows(engine, "SELECT * FROM dev__main.events ORDER BY id")
    assert rows == [
        {"id": "a", "v": 1, "extra": None},  # pre-drift rows NULL-fill the new column
        {"id": "b", "v": 2, "extra": "new-field"},
    ]


async def test_numeric_type_widens_in_place(env: Env) -> None:
    engine, _ = env
    await _run_twice(
        env,
        [
            pa.table({"id": ["a"], "v": pa.array([1], type=pa.int64())}),
            pa.table({"id": ["b"], "v": pa.array([2.5], type=pa.float64())}),
        ],
    )
    rows = await _rows(engine, "SELECT * FROM dev__main.events ORDER BY id")
    assert rows == [{"id": "a", "v": 1.0}, {"id": "b", "v": 2.5}]  # BIGINT promoted to DOUBLE
    [typed] = await _rows(engine, "SELECT typeof(v) AS t FROM dev__main.events LIMIT 1")
    assert typed["t"] == "DOUBLE"


async def test_incompatible_type_is_cast_to_target(env: Env) -> None:
    """A numeric field arriving as strings (a classic API drift) casts back to the
    numeric target — values keep merging, and junk would fail loudly, not corrupt."""
    engine, _ = env
    await _run_twice(
        env,
        [
            pa.table({"id": ["a"], "v": pa.array([1], type=pa.int64())}),
            pa.table({"id": ["b"], "v": pa.array(["7"], type=pa.string())}),
        ],
    )
    rows = await _rows(engine, "SELECT * FROM dev__main.events ORDER BY id")
    assert rows == [{"id": "a", "v": 1}, {"id": "b", "v": 7}]  # '7' cast to BIGINT


async def test_unconvertible_value_fails_loudly(env: Env) -> None:
    engine, store = env
    compiled = compile_models([_model([pa.table({"id": ["a"], "v": [1]}), pa.table({"id": ["b"], "v": ["junk"]})])])
    await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)
    plan = await run_plan(compiled, "dev", store, select={"events"})
    with pytest.raises(Exception, match="(?i)conver"):
        await apply(plan, compiled=compiled, engine=engine, state=store)


async def test_vanished_column_null_fills(env: Env) -> None:
    engine, _ = env
    await _run_twice(
        env,
        [
            pa.table({"id": ["a"], "v": [1], "gone": ["x"]}),
            pa.table({"id": ["b"], "v": [2]}),
        ],
    )
    rows = await _rows(engine, "SELECT * FROM dev__main.events ORDER BY id")
    assert rows == [{"id": "a", "v": 1, "gone": "x"}, {"id": "b", "v": 2, "gone": None}]
