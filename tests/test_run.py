"""Forced execution: interlace run."""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest
import sqlglot
from typer.testing import CliRunner

from interlace.cli.main import app
from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.plan.run import run_plan
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit

runner = CliRunner()
EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "getting_started"


def sql_model(name: str, sql: str, **kwargs: object) -> ModelDef:
    return ModelDef(name=name, sql=sql, **kwargs)  # type: ignore[arg-type]


async def _fetch(engine: DuckDBAdapter, sql: str) -> list[dict]:
    reader = await engine.fetch(sqlglot.parse_one(sql))
    return reader.read_all().to_pylist()


async def test_run_rebuilds_all_even_when_unchanged(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([sql_model("a", "SELECT 1 AS x"), sql_model("b", "SELECT x FROM a")])

    # initial apply, then a no-op plan confirms nothing changed
    await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
    assert (await diff(project, "prod", store)).is_empty

    # run rebuilds everything regardless
    result = await apply(await run_plan(project, "prod", store), compiled=project, engine=engine, state=store)
    assert set(result.built) == {"a", "b"}


async def test_run_merge_picks_up_new_source_data(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS main")
    await engine.execute_sql("CREATE TABLE main.src AS SELECT * FROM (VALUES (1, 'a')) v(id, name)")

    # dim is a merge model reading an external (non-model) source table
    project = compile_models([sql_model("dim", "SELECT id, name FROM main.src", strategy="merge_by_key", key=("id",))])

    await apply(await run_plan(project, "prod", store), compiled=project, engine=engine, state=store)
    assert sorted(await _fetch(engine, "SELECT id, name FROM main.dim"), key=lambda r: r["id"]) == [
        {"id": 1, "name": "a"}
    ]

    # source changes: id=1 updated, id=2 appended
    await engine.execute_sql("UPDATE main.src SET name = 'A' WHERE id = 1")
    await engine.execute_sql("INSERT INTO main.src VALUES (2, 'b')")

    # a second run upserts the new data into the same physical table
    await apply(await run_plan(project, "prod", store), compiled=project, engine=engine, state=store)
    assert sorted(await _fetch(engine, "SELECT id, name FROM main.dim"), key=lambda r: r["id"]) == [
        {"id": 1, "name": "A"},
        {"id": 2, "name": "b"},
    ]


async def test_run_with_selection(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([sql_model("a", "SELECT 1 AS x"), sql_model("b", "SELECT x FROM a")])
    plan = await run_plan(project, "prod", store, select={"a"})
    assert {task.snapshot.name for task in plan.backfills} == {"a"}
    assert plan.promote == ["a"]


def test_run_command_on_example(tmp_path: Path) -> None:
    project_dir = tmp_path / "getting_started"
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace"))

    result = runner.invoke(app, ["run", "--env", "dev", "--path", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert "Ran" in result.output

    con = duckdb.connect(f"ducklake:{project_dir / '.interlace' / 'warehouse.ducklake'}")
    try:
        assert con.execute("SELECT count(*) FROM dev__main.event_totals").fetchone()[0] == 3
    finally:
        con.close()


def test_select_and_restate_commands_on_example(tmp_path: Path) -> None:
    project_dir = tmp_path / "getting_started"
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace"))

    # build the ancestors of event_totals, then restate just that one model
    built = runner.invoke(app, ["run", "--env", "dev", "--path", str(project_dir), "--select", "+event_totals"])
    assert built.exit_code == 0, built.output

    restated = runner.invoke(app, ["restate", "--env", "dev", "--path", str(project_dir), "--select", "event_totals"])
    assert restated.exit_code == 0, restated.output
    assert "Restated" in restated.output
