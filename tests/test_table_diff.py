"""Compare two tables (or a model in two environments) on schema and rows."""

from __future__ import annotations

from pathlib import Path

import pytest

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.plan.table_diff import diff_environments, diff_tables, parse_table_ref
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


def sql_model(name: str, sql: str, **kwargs: object) -> ModelDef:
    return ModelDef(name=name, sql=sql, **kwargs)  # type: ignore[arg-type]


async def test_diff_tables_schema_and_rows(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _store = env
    await engine.execute_sql("CREATE TABLE main.left AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, v)")
    await engine.execute_sql("CREATE TABLE main.right AS SELECT * FROM (VALUES (1, 'A'), (3, 'c')) t(id, v)")
    result = await diff_tables(engine, parse_table_ref("main.left"), parse_table_ref("main.right"), keys=["id"])
    assert result.differs
    assert result.schema.empty
    assert result.rows is not None
    assert result.rows.left_only == 1
    assert result.rows.right_only == 1
    assert result.rows.changed == 1
    assert result.rows.left_count == 2
    assert result.rows.right_count == 2


async def test_diff_tables_type_change(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _store = env
    await engine.execute_sql("CREATE TABLE main.left AS SELECT 1::INTEGER AS id, 'a' AS v")
    await engine.execute_sql("CREATE TABLE main.right AS SELECT 1::BIGINT AS id, 'a' AS v")
    result = await diff_tables(engine, parse_table_ref("main.left"), parse_table_ref("main.right"), keys=["id"])
    assert result.schema.type_changed
    assert result.schema.type_changed[0][0] == "id"


async def test_diff_environments_equal_then_diverges(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    from interlace.engines.registry import as_registry

    engine, store = env
    compiled = compile_models([sql_model("m", "SELECT 1 AS id, 'a' AS v", key=("id",))])
    await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)
    await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)
    registry = as_registry(engine, None)
    same = await diff_environments(
        compiled, left_env="prod", right_env="dev", store=store, engines=registry, keys=["id"]
    )
    assert len(same) == 1
    assert not same[0].differs

    changed = compile_models([sql_model("m", "SELECT 1 AS id, 'b' AS v", key=("id",))])
    await apply(await diff(changed, "dev", store), compiled=changed, engine=engine, state=store)
    diverged = await diff_environments(
        changed, left_env="prod", right_env="dev", store=store, engines=registry, keys=["id"]
    )
    assert diverged[0].differs
    assert diverged[0].rows is not None
    assert diverged[0].rows.changed == 1


def test_cli_diff_json(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from interlace.cli.main import app

    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "m.sql").write_text("/* interlace:\n  key: [id]\n*/\nSELECT 1 AS id, 'a' AS v\n")
    runner = CliRunner()
    assert runner.invoke(app, ["apply", "--path", str(tmp_path)]).exit_code == 0
    assert runner.invoke(app, ["apply", "--env", "dev", "--path", str(tmp_path)]).exit_code == 0
    equal = runner.invoke(app, ["diff", "--against", "dev", "--json", "--path", str(tmp_path)])
    assert equal.exit_code == 0, equal.output
    import json

    body = json.loads(equal.output)
    assert body[0]["differs"] is False

    (tmp_path / "models" / "m.sql").write_text("/* interlace:\n  key: [id]\n*/\nSELECT 1 AS id, 'b' AS v\n")
    forced = runner.invoke(app, ["apply", "--env", "dev", "--force", "--path", str(tmp_path)])
    assert forced.exit_code == 0, forced.output
    changed = runner.invoke(app, ["diff", "--against", "dev", "--json", "--path", str(tmp_path)])
    assert changed.exit_code == 1
    body = json.loads(changed.output)
    assert body[0]["differs"] is True
    assert body[0]["rows"]["changed"] == 1


def test_plan_markdown_marks_breaking(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from interlace.cli.main import app

    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "m.sql").write_text("SELECT 1 AS x")
    runner = CliRunner()
    first = runner.invoke(app, ["plan", "--markdown", "--path", str(tmp_path)])
    assert first.exit_code == 0
    assert "<!-- interlace-plan -->" in first.output
    assert "added" in first.output
    assert runner.invoke(app, ["apply", "--path", str(tmp_path)]).exit_code == 0
    (tmp_path / "models" / "m.sql").write_text("SELECT 2 AS x")
    preview = runner.invoke(app, ["plan", "--markdown", "--path", str(tmp_path)])
    assert preview.exit_code == 0
    assert "breaking" in preview.output
    assert "--force" in preview.output
