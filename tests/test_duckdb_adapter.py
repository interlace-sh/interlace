"""DuckDB engine adapter: Arrow round-trips, DDL, views, and capabilities."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
import sqlglot

from interlace.engines.duckdb import DuckDBAdapter
from interlace.ir.relation import TableRef

pytestmark = pytest.mark.unit


@pytest.fixture()
def adapter() -> Iterator[DuckDBAdapter]:
    a = DuckDBAdapter.in_memory()
    yield a
    a.close()


def _reader(table: pa.Table) -> pa.RecordBatchReader:
    return table.to_reader()


async def test_load_then_fetch_roundtrips_via_arrow(adapter: DuckDBAdapter) -> None:
    src = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    orders = TableRef(schema="main", name="orders")

    await adapter.load(orders, _reader(src), mode="create")
    reader = await adapter.fetch(sqlglot.parse_one("SELECT id, name FROM main.orders ORDER BY id"))
    out = reader.read_all()

    assert out.num_rows == 3
    assert out.column("name").to_pylist() == ["a", "b", "c"]


async def test_create_then_append_accumulates_rows(adapter: DuckDBAdapter) -> None:
    t = TableRef(schema="main", name="events")
    await adapter.load(t, _reader(pa.table({"x": [1, 2]})), mode="create")
    await adapter.load(t, _reader(pa.table({"x": [3, 4, 5]})), mode="append")

    reader = await adapter.fetch(sqlglot.parse_one("SELECT count(*) AS n FROM main.events"))
    assert reader.read_all().column("n").to_pylist() == [5]


async def test_execute_runs_ddl_from_ast(adapter: DuckDBAdapter) -> None:
    await adapter.execute(sqlglot.parse_one("CREATE TABLE main.t AS SELECT 1 AS x"))
    assert await adapter.table_exists(TableRef(schema="main", name="t"))
    assert not await adapter.table_exists(TableRef(schema="main", name="missing"))


async def test_create_view_points_at_table(adapter: DuckDBAdapter) -> None:
    await adapter.load(TableRef(schema="main", name="phys"), _reader(pa.table({"v": [10, 20]})), mode="create")
    await adapter.create_view(TableRef(schema="main", name="v_phys"), TableRef(schema="main", name="phys"))

    reader = await adapter.fetch(sqlglot.parse_one("SELECT sum(v) AS s FROM main.v_phys"))
    assert reader.read_all().column("s").to_pylist() == [30]


async def test_create_schema_and_qualified_load(adapter: DuckDBAdapter) -> None:
    await adapter.create_schema("interlace__silver")
    target = TableRef(schema="interlace__silver", name="orders")
    await adapter.load(target, _reader(pa.table({"id": [1]})), mode="create")
    assert await adapter.table_exists(target)


async def test_describe_returns_columns_and_types(adapter: DuckDBAdapter) -> None:
    await adapter.load(TableRef(schema="main", name="t"), _reader(pa.table({"id": [1], "name": ["a"]})), mode="create")
    described = await adapter.describe(TableRef(schema="main", name="t"))
    assert list(described) == ["id", "name"]  # ordered
    assert described["id"] == "BIGINT"
    assert described["name"] == "VARCHAR"


def test_caps_are_honest_for_duckdb(adapter: DuckDBAdapter) -> None:
    assert adapter.dialect == "duckdb"
    assert adapter.caps.supports_create_or_replace


async def test_write_paths_retry_transaction_conflicts() -> None:
    """DuckLake optimistic-concurrency conflicts (TransactionException) retry:
    the whole idempotent batch re-runs and succeeds once the conflict clears."""
    import duckdb as _duckdb

    from interlace.engines.duckdb import DuckDBAdapter

    class FlakyCursor:
        def __init__(self, owner: FlakyConn) -> None:
            self.owner = owner

        def execute(self, sql: str, *args: object) -> FlakyCursor:
            if self.owner.failures_left > 0 and not sql.startswith(("BEGIN", "ROLLBACK")):
                self.owner.failures_left -= 1
                raise _duckdb.TransactionException("write-write conflict on DuckLake commit")
            self.owner.executed.append(sql)
            return self

        def close(self) -> None: ...

    class FlakyConn:
        def __init__(self, failures: int) -> None:
            self.failures_left = failures
            self.executed: list[str] = []

        def cursor(self) -> FlakyCursor:
            return FlakyCursor(self)

    conn = FlakyConn(failures=2)
    adapter = DuckDBAdapter(conn)  # type: ignore[arg-type]
    await adapter.execute_sql("CREATE TABLE t AS SELECT 1")  # two conflicts, third attempt lands
    assert conn.executed == ["CREATE TABLE t AS SELECT 1"]

    exhausted = FlakyConn(failures=99)
    exhausted_adapter = DuckDBAdapter(exhausted)  # type: ignore[arg-type]
    with pytest.raises(_duckdb.TransactionException):  # gives up after 3 attempts, error surfaces
        await exhausted_adapter.execute_sql("CREATE TABLE t AS SELECT 1")


# --- concurrency: catalog writes are serialised, reads are not -------------------


async def test_concurrent_ddl_keeps_its_schema_qualification() -> None:
    """Regression: DuckLake's catalog is not safe against concurrent DDL on sibling
    cursors of one DatabaseInstance — a ``CREATE TABLE <schema>.<name>`` would
    intermittently lose its schema and land in the catalog's default schema, silently.
    Every table must end up in the schema it named."""
    import asyncio

    adapter = DuckDBAdapter.in_memory()
    try:
        await adapter.create_schema("interlace__raw")
        await asyncio.gather(
            *(adapter.execute_sql(f"CREATE OR REPLACE TABLE interlace__raw.t{i} AS SELECT {i} AS a") for i in range(48))
        )
        reader = await adapter.fetch_sql(
            "SELECT table_schema, count(*) AS n FROM information_schema.tables WHERE table_name LIKE 't%' GROUP BY 1"
        )
        placement = {row["table_schema"]: row["n"] for row in reader.read_all().to_pylist()}
        assert placement == {"interlace__raw": 48}, f"tables escaped their schema: {placement}"
    finally:
        adapter.close()


async def test_catalog_write_serialisation_is_scoped_to_ducklake() -> None:
    """On DuckLake connections the write lock must actually serialise (no two
    catalog-mutating bodies in flight, or the DuckLake race above is reachable);
    on plain DuckDB it must NOT, so parallel builds keep their overlap."""
    import asyncio
    import threading

    depth = 0
    max_depth = 0
    guard = threading.Lock()

    class WatchedCursor:
        def execute(self, sql: str, *args: object) -> WatchedCursor:
            nonlocal depth, max_depth
            with guard:
                depth += 1
                max_depth = max(max_depth, depth)
            try:
                import time

                time.sleep(0.001)  # widen the window a serial lock must close
            finally:
                with guard:
                    depth -= 1
            return self

        def fetchall(self) -> list[tuple[int, ...]]:
            return []

        def close(self) -> None: ...

    class WatchedConn:
        def cursor(self) -> WatchedCursor:
            return WatchedCursor()

    serialised = DuckDBAdapter(WatchedConn(), serialise_writes=True)  # type: ignore[arg-type]
    await asyncio.gather(*(serialised.execute_sql(f"CREATE TABLE t{i} AS SELECT 1") for i in range(16)))
    assert max_depth == 1, f"{max_depth} catalog writes ran concurrently; the write lock is not holding"

    # plain DuckDB has no such catalog bug: writes must genuinely overlap, or apply
    # parallelism is a no-op (measured ~4x on concurrent CTAS)
    depth = max_depth = 0
    parallel = DuckDBAdapter(WatchedConn())  # type: ignore[arg-type]
    await asyncio.gather(*(parallel.execute_sql(f"CREATE TABLE t{i} AS SELECT 1") for i in range(16)))
    assert max_depth > 1, "plain-DuckDB writes were serialised; parallel builds are losing their overlap"


async def test_fetch_closes_its_cursor_when_the_stream_ends() -> None:
    """Regression: fetch used to leak its cursor, leaving the GC to reclaim it on
    whatever thread dropped the last reference while sibling cursors were mid-query.
    The cursor must be closed deterministically when the stream is exhausted."""

    class RecordingCursor:
        def __init__(self, log: list[str]) -> None:
            self.log = log

        def execute(self, sql: str, *args: object) -> RecordingCursor:
            return self

        def to_arrow_reader(self, *args: object) -> pa.RecordBatchReader:
            return pa.table({"i": [1, 2, 3, 4, 5]}).to_reader()

        def close(self) -> None:
            self.log.append("closed")

    class RecordingConn:
        def __init__(self) -> None:
            self.log: list[str] = []

        def cursor(self) -> RecordingCursor:
            return RecordingCursor(self.log)

    conn = RecordingConn()
    adapter = DuckDBAdapter(conn)  # type: ignore[arg-type]
    reader = await adapter.fetch_sql("SELECT * FROM range(5) t(i)")
    assert conn.log == [], "cursor closed before the stream was read"
    assert reader.read_all().num_rows == 5
    assert conn.log == ["closed"], "exhausting the stream must close the cursor"


async def test_sandboxed_fetch_does_not_disable_file_writes(tmp_path: Path) -> None:
    """Regression: the console read path must not poison the warehouse connection.
    DuckDB's enable_external_access is instance-wide and one-way, so the old sandbox
    (a SET on a shared cursor) bricked every later file write — the stream flusher,
    apply, exports. The fence is now the service's AST guard, not an engine latch."""
    engine = DuckDBAdapter.connect(str(tmp_path / "wh.duckdb"))
    try:
        await engine.execute_sql("CREATE TABLE t AS SELECT 1 AS x")
        reader = await engine.fetch_sandboxed(sqlglot.parse_one("SELECT * FROM t", read="duckdb"))
        reader.read_all()  # the console read, drained
        out = tmp_path / "out.parquet"
        await engine.execute_sql(f"COPY (SELECT 1 AS x) TO '{out}' (FORMAT parquet)")  # a file write
        assert out.exists(), "file write after a sandboxed read must still succeed"
    finally:
        engine.close()
