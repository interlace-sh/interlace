"""interlace init: scaffolding a runnable project."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from interlace.cli.main import app
from interlace.exceptions import ConfigurationError
from interlace.project import Project
from interlace.scaffold import scaffold_project

pytestmark = pytest.mark.unit

runner = CliRunner()


def test_scaffold_writes_a_loadable_project(tmp_path: Path) -> None:
    written = scaffold_project(tmp_path, name="shop")

    assert (tmp_path / "interlace.yaml") in written
    assert (tmp_path / "models" / "raw_events.sql").exists()

    project = Project.load(tmp_path)
    assert project.config.name == "shop"
    compiled = project.compile()
    assert "event_totals" in compiled.models
    assert compiled.models["event_totals"].dependencies == ("raw_events",)


def test_scaffold_refuses_to_overwrite(tmp_path: Path) -> None:
    scaffold_project(tmp_path)
    with pytest.raises(ConfigurationError):
        scaffold_project(tmp_path)


def test_init_command_then_plan(tmp_path: Path) -> None:
    init_result = runner.invoke(app, ["init", str(tmp_path), "--name", "demo"])
    assert init_result.exit_code == 0, init_result.output
    assert (tmp_path / "interlace.yaml").exists()

    plan_result = runner.invoke(app, ["plan", "--env", "dev", "--path", str(tmp_path)])
    assert plan_result.exit_code == 0, plan_result.output
    assert "raw_events" in plan_result.output


def test_init_command_refuses_existing_project(tmp_path: Path) -> None:
    runner.invoke(app, ["init", str(tmp_path)])
    second = runner.invoke(app, ["init", str(tmp_path)])
    assert second.exit_code == 1
