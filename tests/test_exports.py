"""Terminal `materialise: file` models write their result to a path (no managed table)."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef, validate_materialise
from interlace.dsl.discovery import discover_models
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import DefinitionError, PlanError
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.sinks import file_statements
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


def file_model(name: str, sql: str, fmt: str, path: str) -> ModelDef:
    return ModelDef(name=name, sql=sql, materialise="file", format=fmt, path=path)


def test_file_requires_path_and_format() -> None:
    validate_materialise("m", materialise="file", strategy="replace", target=None, path="x.csv", format="csv", key=())
    with pytest.raises(DefinitionError, match="needs a path"):
        validate_materialise("m", materialise="file", strategy="replace", target=None, path=None, format="csv", key=())
    with pytest.raises(DefinitionError, match="needs format"):
        validate_materialise("m", materialise="file", strategy="replace", target=None, path="x", format=None, key=())
    with pytest.raises(DefinitionError, match="only strategy: replace"):
        validate_materialise("m", materialise="file", strategy="append", target=None, path="x", format="csv", key=())


def test_file_statements_rejects_unknown_format() -> None:
    with pytest.raises(PlanError):
        file_statements("salesforce", sqlglot.parse_one("SELECT 1"), "x", "duckdb")


def test_removed_export_key_raises_migration_error(tmp_path: Path) -> None:
    """A stray `export:` block in a SQL header points at the 2.0 replacement."""
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "m.sql").write_text(
        "/* interlace: {export: {to: parquet, path: out.parquet}} */\nSELECT 1 AS id"
    )
    with pytest.raises(DefinitionError, match="export: was removed in 2.0"):
        discover_models(tmp_path, ["models"], "duckdb")


async def test_file_to_parquet(env: tuple[DuckDBAdapter, SqliteStateStore], tmp_path: Path) -> None:
    engine, store = env
    out = tmp_path / "out" / "data.parquet"
    project = compile_models([file_model("dump", "SELECT 1 AS id, 'a' AS name", "parquet", str(out))])

    result = await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)

    assert result.built == ["dump"]
    assert out.exists()
    con = duckdb.connect()
    try:
        assert con.execute(f"SELECT id, name FROM '{out}'").fetchall() == [(1, "a")]
    finally:
        con.close()


async def test_file_to_csv(env: tuple[DuckDBAdapter, SqliteStateStore], tmp_path: Path) -> None:
    engine, store = env
    out = tmp_path / "data.csv"
    project = compile_models([file_model("dump", "SELECT 1 AS id, 2 AS v", "csv", str(out))])
    await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
    assert out.read_text() == "id,v\n1,2\n"


async def test_file_has_no_table_or_view(env: tuple[DuckDBAdapter, SqliteStateStore], tmp_path: Path) -> None:
    engine, store = env
    project = compile_models([file_model("dump", "SELECT 1 AS id", "json", str(tmp_path / "d.json"))])

    plan = await diff(project, "prod", store)
    assert {t.snapshot.name for t in plan.backfills} == {"dump"}  # it does build (run the delivery)
    assert plan.virtual_updates == []  # but no environment view

    await apply(plan, compiled=project, engine=engine, state=store)
    from interlace.ir.relation import TableRef

    assert not await engine.table_exists(project.models["dump"].physical_table)  # no snapshot table
    assert not await engine.table_exists(TableRef(schema="prod__main", name="dump"))  # no env view
    assert "dump" in await store.get_environment("prod")  # fingerprint still tracked


async def test_file_reads_upstream_model(env: tuple[DuckDBAdapter, SqliteStateStore], tmp_path: Path) -> None:
    engine, store = env
    out = tmp_path / "orders.json"
    project = compile_models(
        [
            ModelDef(name="orders", sql="SELECT * FROM (VALUES (1), (2)) v(id)"),
            file_model("orders_out", "SELECT id FROM orders", "json", str(out)),
        ]
    )
    await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)

    lines = [json.loads(line) for line in out.read_text().splitlines()]
    assert sorted(r["id"] for r in lines) == [1, 2]
