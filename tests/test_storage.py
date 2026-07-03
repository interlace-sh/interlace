"""Warehouse storage backends: DuckLake (the default) and quack (remote serving).

DuckLake: a project with the default config gets a Parquet-backed warehouse with
a DuckLake catalog — plan/apply must behave identically to a plain DuckDB file.
Quack: a second process (here: a second adapter in this process) reaches the same
warehouse through the quack protocol, including a full plan/apply round-trip.
"""

from __future__ import annotations

import shutil
import socket
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.engines.quack import QuackAdapter, sql_literal
from interlace.graph.project import compile_models
from interlace.ir.relation import TableRef
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.project import Project
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.integration

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "getting_started"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _rows(engine: DuckDBAdapter, sql: str) -> list[dict]:
    reader = await engine.fetch(sqlglot.parse_one(sql))
    return reader.read_all().to_pylist()


# --- DuckLake (default storage) ----------------------------------------------


async def test_default_config_applies_onto_ducklake(tmp_path: Path) -> None:
    project_dir = tmp_path / "getting_started"
    shutil.copytree(EXAMPLE, project_dir)
    project = Project.load(project_dir)
    assert project.config.database.startswith("ducklake:")

    compiled = project.compile()
    engine = project.open_engine()
    state = await project.open_state()
    try:
        result = await apply(await diff(compiled, "dev", state), compiled=compiled, engine=engine, state=state)
        assert "event_totals" in result.built
        rows = await _rows(engine, "SELECT kind, events FROM dev__main.event_totals ORDER BY events DESC")
        assert rows and {row["kind"] for row in rows} >= {"click", "view"}
    finally:
        await state.close()
        engine.close()

    # the warehouse is a DuckLake: a catalog file plus a data directory (small
    # tables are inlined in the catalog; Parquet appears as data grows)
    catalog = project_dir / ".interlace" / "warehouse.ducklake"
    assert catalog.exists()
    assert (project_dir / ".interlace" / "warehouse.ducklake.files").is_dir()


# --- Quack (remote warehouse) --------------------------------------------------


@pytest.fixture()
async def quack_warehouse(tmp_path: Path) -> AsyncIterator[tuple[str, str, DuckDBAdapter]]:
    """A DuckLake warehouse served over quack from this process."""
    port = _free_port()
    uri, token = f"quack:localhost:{port}", "test-token-123"
    server = DuckDBAdapter.connect(f"ducklake:{tmp_path / 'warehouse.ducklake'}")
    await server.execute_sql(f"CALL quack_serve({sql_literal(uri)}, token := {sql_literal(token)})")
    yield uri, token, server
    await server.execute_sql(f"CALL quack_stop({sql_literal(uri)})")
    server.close()


async def test_quack_adapter_full_surface(quack_warehouse: tuple[str, str, DuckDBAdapter]) -> None:
    uri, token, _server = quack_warehouse
    client = QuackAdapter.connect(uri, token=token)
    try:
        await client.create_schema("interlace__main")
        await client.execute_sql("CREATE TABLE interlace__main.t AS SELECT 1 AS id, 'a' AS v")
        table = TableRef(schema="interlace__main", name="t")

        assert await client.table_exists(table)
        assert not await client.table_exists(TableRef(schema="interlace__main", name="nope"))
        assert await client.describe(table) == {"id": "INTEGER", "v": "VARCHAR"}

        reader = await client.fetch_sql("SELECT * FROM interlace__main.t")
        assert reader.read_all().to_pylist() == [{"id": 1, "v": "a"}]

        # atomic multi-statement: a failing statement rolls the whole payload back
        with pytest.raises(Exception):  # noqa: B017 — any server-side error
            await client.execute_all(
                [
                    sqlglot.parse_one("CREATE TABLE interlace__main.atomic AS SELECT 1 AS x"),
                    sqlglot.parse_one("SELECT * FROM interlace__main.does_not_exist"),
                ]
            )
        assert not await client.table_exists(TableRef(schema="interlace__main", name="atomic"))
    finally:
        client.close()


async def test_quack_adapter_arrow_load(quack_warehouse: tuple[str, str, DuckDBAdapter]) -> None:
    import pyarrow as pa

    uri, token, _server = quack_warehouse
    client = QuackAdapter.connect(uri, token=token)
    try:
        await client.create_schema("interlace__main")
        table = TableRef(schema="interlace__main", name="loaded")
        await client.load(table, pa.table({"n": [1, 2]}).to_reader(), "create")
        await client.load(table, pa.table({"n": [3]}).to_reader(), "append")
        reader = await client.fetch_sql("SELECT n FROM interlace__main.loaded ORDER BY n")
        assert [r["n"] for r in reader.read_all().to_pylist()] == [1, 2, 3]
    finally:
        client.close()


async def test_plan_apply_through_quack(quack_warehouse: tuple[str, str, DuckDBAdapter], tmp_path: Path) -> None:
    uri, token, _server = quack_warehouse
    client = QuackAdapter.connect(uri, token=token)
    store = await SqliteStateStore.open(tmp_path / "state.db")
    try:
        compiled = compile_models(
            [
                ModelDef(name="a", sql="SELECT 1 AS id, 10 AS v"),
                ModelDef(name="b", sql="SELECT id, v * 2 AS v2 FROM a"),
            ]
        )
        result = await apply(await diff(compiled, "prod", store), compiled=compiled, engine=client, state=store)
        assert set(result.built) == {"a", "b"}
        rows = await _rows(client, "SELECT id, v2 FROM prod__main.b")
        assert rows == [{"id": 1, "v2": 20}]
    finally:
        await store.close()
        client.close()
