"""Warehouse storage backends: plain DuckDB (the default), DuckLake, and quack.

Default: a project with no `database:` gets a plain single-file DuckDB warehouse.
DuckLake: opting in with `database: ducklake:…` gets a Parquet-backed warehouse
with a catalog — plan/apply must behave identically either way.
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
from conftest import fetch_rows as _rows

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


# --- Warehouse storage --------------------------------------------------------


async def _apply_example(project_dir: Path) -> None:
    """Apply the getting_started example in `dev` and assert it built."""
    project = Project.load(project_dir)
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


def _example(tmp_path: Path, database: str | None = None) -> Path:
    project_dir = tmp_path / "getting_started"
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace"))
    if database is not None:
        config = project_dir / "interlace.yaml"
        config.write_text(f"{config.read_text().rstrip()}\ndatabase: {database}\n")
    return project_dir


async def test_default_config_applies_onto_a_duckdb_file(tmp_path: Path) -> None:
    """The default warehouse is one plain DuckDB file — no catalog, no data directory."""
    project_dir = _example(tmp_path)
    assert Project.load(project_dir).config.database == ".interlace/warehouse.duckdb"

    await _apply_example(project_dir)

    assert (project_dir / ".interlace" / "warehouse.duckdb").is_file()
    assert not (project_dir / ".interlace" / "warehouse.ducklake").exists()


async def test_ducklake_warehouse_applies_identically(tmp_path: Path) -> None:
    """DuckLake stays first-class, one config line away: same plan/apply, different store."""
    project_dir = _example(tmp_path, database="ducklake:.interlace/warehouse.ducklake")

    await _apply_example(project_dir)

    # a DuckLake is a catalog file plus a data directory (small tables are inlined
    # in the catalog; Parquet appears as data grows)
    assert (project_dir / ".interlace" / "warehouse.ducklake").exists()
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
        rows = await _rows(client, "SELECT id, v2 FROM main.b")
        assert rows == [{"id": 1, "v2": 20}]
    finally:
        await store.close()
        client.close()
