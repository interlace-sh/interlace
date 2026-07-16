"""Python model execution: Arrow in (RelationHandles), Arrow out, loaded at the sink."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef, model
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import DefinitionError, PlanError, SchemaError
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.runtime.handles import RelationHandle
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


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


async def _build(env: tuple[DuckDBAdapter, SqliteStateStore], models: list[ModelDef]) -> None:
    engine, store = env
    compiled = compile_models(models)
    await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)


RAW = ModelDef(name="raw", sql="SELECT * FROM (VALUES (1, 10), (2, 20), (3, 5)) AS t (id, amount)")


async def test_sync_function_returning_table(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def doubled(raw: RelationHandle) -> pa.Table:
        table = raw.table()
        return table.set_column(1, "amount", pc.multiply(table["amount"], 2))

    await _build(env, [RAW, ModelDef(name="doubled", fn=doubled, depends_on=("raw",))])
    rows = await _rows(env[0], "SELECT * FROM dev__main.doubled ORDER BY id")
    assert rows == [{"id": 1, "amount": 20}, {"id": 2, "amount": 40}, {"id": 3, "amount": 10}]


async def test_async_function_returning_reader(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    async def passthrough(raw: RelationHandle) -> pa.RecordBatchReader:
        return raw.reader()

    await _build(env, [RAW, ModelDef(name="passthrough", fn=passthrough, depends_on=("raw",))])
    rows = await _rows(env[0], "SELECT count(*) AS n FROM dev__main.passthrough")
    assert rows == [{"n": 3}]


async def test_generator_streams_batches(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def filtered(raw: RelationHandle) -> Iterator[pa.RecordBatch]:
        for batch in raw.reader():
            mask = pc.greater(batch.column("amount"), 8)
            yield batch.filter(mask)

    await _build(env, [RAW, ModelDef(name="filtered", fn=filtered, depends_on=("raw",))])
    rows = await _rows(env[0], "SELECT id FROM dev__main.filtered ORDER BY id")
    assert [r["id"] for r in rows] == [1, 2]


async def test_python_model_over_ephemeral_upstream(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    staged = ModelDef(name="staged", sql="SELECT id, amount FROM raw WHERE amount >= 10", materialise="ephemeral")

    def total(staged: RelationHandle) -> pa.Table:
        return pa.table({"total": [pc.sum(staged.table()["amount"]).as_py()]})

    await _build(env, [RAW, staged, ModelDef(name="total", fn=total, depends_on=("staged",))])
    assert await _rows(env[0], "SELECT total FROM dev__main.total") == [{"total": 30}]


async def test_downstream_sql_reads_python_output(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def doubled(raw: RelationHandle) -> pa.Table:
        table = raw.table()
        return table.set_column(1, "amount", pc.multiply(table["amount"], 2))

    await _build(
        env,
        [
            RAW,
            ModelDef(name="doubled", fn=doubled, depends_on=("raw",)),
            ModelDef(name="big", sql="SELECT id FROM doubled WHERE amount > 15"),
        ],
    )
    rows = await _rows(env[0], "SELECT id FROM dev__main.big ORDER BY id")
    assert [r["id"] for r in rows] == [1, 2]


async def test_contract_validated_on_python_output(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def bad(raw: RelationHandle) -> pa.Table:
        return raw.table().drop_columns(["amount"])

    bad_model = ModelDef(name="bad", fn=bad, depends_on=("raw",), columns={"id": None, "amount": None})
    with pytest.raises(SchemaError):
        await _build(env, [RAW, bad_model])


async def test_unknown_parameter_is_rejected(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def broken(nonexistent: RelationHandle) -> pa.Table:
        return nonexistent.table()

    with pytest.raises(DefinitionError, match="not declared dependencies"):
        await _build(env, [RAW, ModelDef(name="broken", fn=broken, depends_on=("raw",))])


async def test_python_model_supports_keyed_strategies(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def keyed(raw: RelationHandle) -> pa.Table:
        return raw.table()

    keyed_model = ModelDef(name="keyed", fn=keyed, depends_on=("raw",), strategy="merge_by_key", key=("id",))
    await _build(env, [RAW, keyed_model])
    rows = await _rows(env[0], "SELECT id FROM dev__main.keyed ORDER BY id")
    assert [row["id"] for row in rows] == [1, 2, 3]


async def test_python_model_rejects_incremental_by_time(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def windowed(raw: RelationHandle) -> pa.Table:
        return raw.table()

    windowed_model = ModelDef(
        name="windowed", fn=windowed, depends_on=("raw",), strategy="incremental_by_time", time_column="ts"
    )
    with pytest.raises(PlanError, match="incremental_by_time"):
        await _build(env, [RAW, windowed_model])


async def test_handle_is_single_pass(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def greedy(raw: RelationHandle) -> pa.Table:
        raw.table()
        return raw.table()  # second read must fail loudly

    with pytest.raises(DefinitionError, match="already consumed"):
        await _build(env, [RAW, ModelDef(name="greedy", fn=greedy, depends_on=("raw",))])


def test_decorator_rejects_python_views() -> None:
    with pytest.raises(DefinitionError, match="cannot be views"):

        @model(materialise="view")
        def as_view() -> None: ...
