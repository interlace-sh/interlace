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
    shutil.copytree(EXAMPLE, target)
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
