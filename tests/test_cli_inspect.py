"""CLI inspection commands: list and lineage."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from interlace.cli.main import app

pytestmark = pytest.mark.unit

runner = CliRunner()
EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "getting_started"


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    target = tmp_path / "getting_started"
    shutil.copytree(EXAMPLE, target, ignore=shutil.ignore_patterns(".interlace"))
    return target


def test_list_shows_models_and_outputs(project: Path) -> None:
    result = runner.invoke(app, ["list", "--path", str(project)])
    assert result.exit_code == 0, result.output
    assert "raw_events" in result.output
    assert "event_totals" in result.output
    assert "view" in result.output  # recent_clicks is a view


def test_list_honours_selection(project: Path) -> None:
    result = runner.invoke(app, ["list", "--path", str(project), "--select", "raw_events"])
    assert result.exit_code == 0, result.output
    assert "raw_events" in result.output
    assert "event_totals" not in result.output


def test_lineage_shows_upstream_and_downstream(project: Path) -> None:
    result = runner.invoke(app, ["lineage", "event_totals", "--path", str(project)])
    assert result.exit_code == 0, result.output
    assert "raw_events" in result.output  # upstream
    assert "top_kind" in result.output  # downstream


def test_lineage_unknown_model_errors(project: Path) -> None:
    result = runner.invoke(app, ["lineage", "nope", "--path", str(project)])
    assert result.exit_code == 1


def test_envs_runs_checks_engines_streams_commands(tmp_path: Path) -> None:
    """The inspection surface: every read-only command renders against a real project."""
    project = tmp_path / "proj"
    (project / "models").mkdir(parents=True)
    (project / "interlace.yaml").write_text(
        "name: inspect\ndatabase: ':memory:'\nengines:\n  side:\n    type: duckdb\n    database: side.duckdb\n"
    )
    (project / "models" / "m.sql").write_text("/* interlace: {checks: [{not_null: x}]} */\nSELECT 1 AS x")
    (project / "models" / "s.py").write_text(
        'from interlace import stream\n\n@stream("clicks", schema={"n": "int"})\ndef clicks(e):\n    return e\n'
    )

    def run(*args: str) -> str:
        result = runner.invoke(app, [*args, "--path", str(project)])
        assert result.exit_code == 0, result.output
        return result.output

    assert "No environments promoted yet" in run("envs")
    run("apply")  # default env: production, unprefixed views
    envs_out = run("envs")
    assert "prod" in envs_out and "main.* (production)" in envs_out
    assert "not_null_x" in run("checks")
    assert "clicks" in run("streams")
    engines_out = run("engines")
    assert "default" in engines_out and "side" in engines_out
    assert "Runs" in run("runs")
