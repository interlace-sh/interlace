"""The shared scratch database, and releasing a Python model's Arrow result."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest

from interlace.dsl.decorators import ModelDef
from interlace.engines.base import EngineAdapter, EngineCaps
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.ir.relation import TableRef
from interlace.plan.backfill import _merge_python_output
from interlace.readers import own_reader
from interlace.runtime.python_model import run_python_model
from interlace.runtime.scratch import bind, reset, session

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _close_scratch() -> Iterator[None]:
    reset()
    yield
    reset()


def test_sessions_share_one_database() -> None:
    with session() as cursor:
        cursor.execute("CREATE TABLE kept AS SELECT 1 AS id")
    with session() as cursor:
        assert cursor.execute("SELECT id FROM kept").fetchone() == (1,)


def test_sessions_run_one_at_a_time() -> None:
    order: list[str] = []
    entered = threading.Event()

    def first() -> None:
        with session() as cursor:
            order.append("first-enter")
            entered.set()
            cursor.execute("SELECT 1").fetchall()
            time.sleep(0.15)
            order.append("first-leave")

    def second() -> None:
        assert entered.wait(timeout=2)
        with session() as cursor:
            order.append("second-enter")
            cursor.execute("SELECT 1").fetchall()

    background = threading.Thread(target=second)
    background.start()
    first()
    background.join(timeout=2)
    assert order == ["first-enter", "first-leave", "second-enter"]


async def test_bind_reaches_the_worker_that_runs_a_sync_read(tmp_path: Path) -> None:
    engine = DuckDBAdapter.in_memory()
    engine.apply_settings({"memory_limit": "256MiB", "threads": 1, "temp_directory": str(tmp_path)})
    seen: dict[str, object] = {}

    async def model() -> None:
        def read() -> None:
            with session() as cursor:
                row = cursor.execute("SELECT current_setting('threads'), current_setting('temp_directory')").fetchone()
                assert row is not None
                seen["threads"] = row[0]
                seen["temp"] = row[1]

        await asyncio.to_thread(read)

    with bind(engine):
        await model()
    assert int(seen["threads"]) == 1  # type: ignore[arg-type]
    assert str(seen["temp"]).endswith("scratch")
    engine.close()


def test_load_releases_a_reader_the_query_never_scanned() -> None:
    """A failed load still releases. The caller may be about to drop its only reference."""
    engine = DuckDBAdapter.in_memory()
    released: list[str] = []
    reader = own_reader(
        pa.table({"id": [1]}).to_reader(),
        release=lambda: released.append("done"),
    )

    def boom(table: TableRef, source: pa.RecordBatchReader, mode: str) -> int:
        del table, source, mode
        raise RuntimeError("boom")

    engine._load_sync = boom  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(engine.load(TableRef(schema="main", name="t"), reader, "create"))
    assert released == ["done"]
    engine.close()


class _RecordingEngine:
    """Stands in for the warehouse so the test can see which side runs first."""

    dialect = "duckdb"
    caps = EngineCaps()
    released_before_sql = False

    async def load(self, table: TableRef, reader: pa.RecordBatchReader, mode: str) -> int:
        del table, reader, mode
        return 1

    async def execute_all(self, statements: list[object]) -> list[int]:
        self.released_before_sql = _released[0]
        return [0] * len(statements)


_released = [False]


async def test_keyed_python_output_is_released_before_the_merge() -> None:
    _released[0] = False
    table = pa.table({"id": [1], "val": ["a"]})

    def model() -> pa.RecordBatchReader:
        return own_reader(table.to_reader(), release=lambda: _released.__setitem__(0, True))

    compiled = compile_models([ModelDef(name="listing", fn=model, strategy="full_merge", key=("id",))])
    compiled_model = compiled.models["listing"]
    warehouse = _RecordingEngine()
    engine: EngineAdapter = warehouse  # type: ignore[assignment]
    await _merge_python_output(
        compiled_model,
        engine,
        compiled_model.physical_table,
        await run_python_model(compiled_model, compiled, engine),
        exists=False,
    )
    assert warehouse.released_before_sql
