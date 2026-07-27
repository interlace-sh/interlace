"""Sinks / exports: a model with an `export` block writes to a destination."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import ConfigurationError, PlanError
from interlace.exports import ExportConfig, export_statements
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


def sink(name: str, sql: str, to: str, path: str) -> ModelDef:
    return ModelDef(name=name, sql=sql, export=ExportConfig(to=to, path=path))


def test_export_config_requires_to_and_path() -> None:
    assert ExportConfig.from_dict({"to": "csv", "path": "x.csv"}) == ExportConfig("csv", "x.csv")
    with pytest.raises(ConfigurationError):
        ExportConfig.from_dict({"to": "csv"})


def test_export_statements_rejects_unknown_destination() -> None:
    with pytest.raises(PlanError):
        export_statements(ExportConfig("salesforce", "x"), sqlglot.parse_one("SELECT 1"), "x", "duckdb")


async def test_export_to_parquet(env: tuple[DuckDBAdapter, SqliteStateStore], tmp_path: Path) -> None:
    engine, store = env
    out = tmp_path / "out" / "data.parquet"
    project = compile_models([sink("dump", "SELECT 1 AS id, 'a' AS name", "parquet", str(out))])

    result = await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)

    assert result.built == ["dump"]
    assert out.exists()
    con = duckdb.connect()
    try:
        assert con.execute(f"SELECT id, name FROM '{out}'").fetchall() == [(1, "a")]
    finally:
        con.close()


async def test_export_to_csv(env: tuple[DuckDBAdapter, SqliteStateStore], tmp_path: Path) -> None:
    engine, store = env
    out = tmp_path / "data.csv"
    project = compile_models([sink("dump", "SELECT 1 AS id, 2 AS v", "csv", str(out))])
    await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
    assert out.read_text() == "id,v\n1,2\n"


async def test_sink_has_no_table_or_view(env: tuple[DuckDBAdapter, SqliteStateStore], tmp_path: Path) -> None:
    engine, store = env
    project = compile_models([sink("dump", "SELECT 1 AS id", "json", str(tmp_path / "d.json"))])

    plan = await diff(project, "prod", store)
    assert {t.snapshot.name for t in plan.backfills} == {"dump"}  # it does build (run the export)
    assert plan.virtual_updates == []  # but no environment view

    await apply(plan, compiled=project, engine=engine, state=store)
    from interlace.ir.relation import TableRef

    assert not await engine.table_exists(project.models["dump"].physical_table)  # no snapshot table
    assert not await engine.table_exists(TableRef(schema="prod__main", name="dump"))  # no env view
    assert "dump" in await store.get_environment("prod")  # fingerprint still tracked


async def test_sink_reads_upstream_model(env: tuple[DuckDBAdapter, SqliteStateStore], tmp_path: Path) -> None:
    engine, store = env
    out = tmp_path / "orders.json"
    project = compile_models(
        [
            ModelDef(name="orders", sql="SELECT * FROM (VALUES (1), (2)) v(id)"),
            sink("orders_out", "SELECT id FROM orders", "json", str(out)),
        ]
    )
    await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)

    lines = [json.loads(line) for line in out.read_text().splitlines()]
    assert sorted(r["id"] for r in lines) == [1, 2]
