"""CLI plan/apply against a real project directory."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from interlace.cli.main import app

pytestmark = pytest.mark.unit

runner = CliRunner()


def _project(root: Path) -> None:
    (root / "models").mkdir(parents=True, exist_ok=True)
    (root / "models" / "a.sql").write_text("SELECT 1 AS id, 10 AS v")
    (root / "models" / "b.sql").write_text("SELECT id, v * 2 AS v2 FROM a")


def test_plan_lists_added_models(tmp_path: Path) -> None:
    _project(tmp_path)
    result = runner.invoke(app, ["plan", "--env", "prod", "--path", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "a" in result.output and "b" in result.output
    assert "added" in result.output


def test_apply_builds_then_replan_is_clean(tmp_path: Path) -> None:
    _project(tmp_path)

    applied = runner.invoke(app, ["apply", "--env", "prod", "--path", str(tmp_path)])
    assert applied.exit_code == 0, applied.output
    assert "Built 2 model(s)" in applied.output

    # the warehouse file now holds the env view with the computed value
    con = duckdb.connect(f"ducklake:{tmp_path / '.interlace' / 'warehouse.ducklake'}")
    try:
        rows = con.execute("SELECT id, v2 FROM prod__main.b").fetchall()
    finally:
        con.close()
    assert rows == [(1, 20)]

    replan = runner.invoke(app, ["plan", "--env", "prod", "--path", str(tmp_path)])
    assert replan.exit_code == 0
    assert "No changes" in replan.output


def test_plan_on_empty_project_is_clean(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    result = runner.invoke(app, ["plan", "--env", "dev", "--path", str(tmp_path)])
    assert result.exit_code == 0
    assert "No changes" in result.output
