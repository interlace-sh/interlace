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


def test_models_shows_models_and_outputs(project: Path) -> None:
    result = runner.invoke(app, ["models", "--path", str(project)])
    assert result.exit_code == 0, result.output
    assert "raw_events" in result.output
    assert "event_totals" in result.output
    assert "view" in result.output  # recent_clicks is a view


def test_list_is_a_hidden_alias_for_models(project: Path) -> None:
    result = runner.invoke(app, ["list", "--path", str(project)])
    assert result.exit_code == 0, result.output
    assert "raw_events" in result.output


def test_models_honours_selection(project: Path) -> None:
    result = runner.invoke(app, ["models", "--path", str(project), "--select", "raw_events"])
    assert result.exit_code == 0, result.output
    assert "raw_events" in result.output
    assert "event_totals" not in result.output


def test_models_json_output(project: Path) -> None:
    import json

    result = runner.invoke(app, ["models", "--path", str(project), "--json"])
    assert result.exit_code == 0, result.output
    rows = json.loads(result.output)
    by_name = {row["name"]: row for row in rows}
    assert by_name["recent_clicks"]["output"] == "view"
    assert "raw_events" in by_name["event_totals"]["depends_on"]


def test_plan_json_output(project: Path) -> None:
    import json

    result = runner.invoke(app, ["plan", "--env", "prod", "--path", str(project), "--json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["environment"] == "prod"
    changes = {c["name"]: c for c in data["changes"]}
    assert changes["event_totals"]["change_type"] == "added"
    assert changes["event_totals"]["new_fingerprint"]


def test_lineage_shows_upstream_and_downstream(project: Path) -> None:
    result = runner.invoke(app, ["lineage", "event_totals", "--path", str(project)])
    assert result.exit_code == 0, result.output
    assert "raw_events" in result.output  # upstream
    assert "top_kind" in result.output  # downstream


def test_lineage_dot_output(project: Path) -> None:
    result = runner.invoke(app, ["lineage", "event_totals", "--path", str(project), "--format", "dot"])
    assert result.exit_code == 0, result.output
    assert result.output.startswith("digraph lineage {")
    assert '"raw_events" -> "event_totals"' in result.output


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

    assert "No environments promoted yet" in run("env", "list")
    run("apply")  # default env: production, unprefixed views
    envs_out = run("env", "list")
    assert "prod" in envs_out and "main.* (production)" in envs_out
    assert "not_null_x" in run("checks", "list")
    assert "clicks" in run("streams")
    engines_out = run("engines")
    assert "default" in engines_out and "side" in engines_out
    # nothing enqueued yet: the empty state explains itself instead of a bare table
    assert "No runs recorded" in run("runs")

    # environment lifecycle: sandboxes drop freely, production is guarded
    run("apply", "--env", "dev")
    assert "dev" in run("env", "list")
    dropped = run("env", "drop", "dev")
    assert "Dropped environment" in dropped
    assert "dev" not in run("env", "list").replace("dev__", "")  # row gone (ignore view-name column)
    guarded = runner.invoke(app, ["env", "drop", "prod", "--path", str(project)])
    assert guarded.exit_code == 1 and "production" in guarded.output


def test_checks_run_fails_on_blocking_check(tmp_path: Path) -> None:
    """checks run exits 1 when an error-severity check fails against the promoted data."""
    import json

    project = tmp_path / "proj"
    (project / "models").mkdir(parents=True)
    (project / "interlace.yaml").write_text("name: gate\ndatabase: wh.duckdb\n")  # promoted tables must persist
    model = project / "models" / "m.sql"
    model.write_text("/* interlace: {checks: [{accepted_values: {column: x, values: [1]}}]} */\nSELECT 1 AS x")
    assert runner.invoke(app, ["apply", "--path", str(project)]).exit_code == 0

    passing = runner.invoke(app, ["checks", "run", "--path", str(project)])
    assert passing.exit_code == 0, passing.output
    assert "1/1 passed" in passing.output

    # tighten the check so the SAME promoted data now fails it — no rebuild involved
    model.write_text("/* interlace: {checks: [{accepted_values: {column: x, values: [2]}}]} */\nSELECT 1 AS x")
    failing = runner.invoke(app, ["checks", "run", "--path", str(project)])
    assert failing.exit_code == 1, failing.output
    assert "failed" in failing.output

    as_json = runner.invoke(app, ["checks", "run", "--path", str(project), "--json"])
    outcomes = json.loads(as_json.output)
    assert outcomes[0]["status"] == "failed"


def test_run_rejects_bad_iso_window(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    (tmp_path / "interlace.yaml").write_text("name: iso\ndatabase: ':memory:'\n")
    result = runner.invoke(app, ["run", "--path", str(tmp_path), "--start", "not-a-time"])
    assert result.exit_code == 2
    assert "ISO timestamp" in result.output
