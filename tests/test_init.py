"""interlace init: scaffolding a runnable project."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from interlace.cli.main import app
from interlace.exceptions import ConfigurationError
from interlace.project import Project
from interlace.scaffold import DEFAULT_TEMPLATE, list_templates, scaffold_project

pytestmark = pytest.mark.unit

runner = CliRunner()


def test_scaffold_writes_a_loadable_project(tmp_path: Path) -> None:
    written = scaffold_project(tmp_path, name="shop")

    assert (tmp_path / "interlace.yaml") in written
    assert (tmp_path / "models" / "raw_events.sql").exists()
    assert (tmp_path / "models" / "enriched_events.py").exists()
    assert not (tmp_path / "template.yaml").exists()  # metadata is not copied into the project

    project = Project.load(tmp_path)
    assert project.config.name == "shop"
    compiled = project.compile()
    # SQL seed -> Python model (dependency inferred from the parameter name) -> SQL rollup
    assert compiled.models["enriched_events"].dependencies == ("raw_events",)
    assert compiled.models["event_summary"].dependencies == ("enriched_events",)


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


def test_list_templates_puts_the_default_first() -> None:
    names = [t.name for t in list_templates()]
    assert names and names[0] == DEFAULT_TEMPLATE
    assert "quickstart" in names


def test_init_list_flag_shows_templates() -> None:
    result = runner.invoke(app, ["init", "--list"])
    assert result.exit_code == 0, result.output
    assert "quickstart" in result.output


def test_scaffold_unknown_template_errors(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="unknown template"):
        scaffold_project(tmp_path, template="does-not-exist")


def test_init_with_explicit_template(tmp_path: Path) -> None:
    result = runner.invoke(app, ["init", str(tmp_path), "--template", "quickstart", "--name", "q"])
    assert result.exit_code == 0, result.output
    assert Project.load(tmp_path).config.name == "q"


def test_github_template_compiles_as_an_incremental_source(tmp_path: Path) -> None:
    scaffold_project(tmp_path, name="gh", template="github")
    compiled = Project.load(tmp_path).compile()  # imports the model → interlace.sources must import
    issues = compiled.models["github_issues"]
    assert issues.cursor == "updated_at" and issues.strategy == "merge" and issues.key == ("id",)
    assert issues.dependencies == ()  # a source has no upstream model
    assert compiled.models["issues_by_state"].dependencies == ("github_issues",)
