"""
Tests for CLI commands.

Uses typer's CliRunner to test CLI commands without actual execution.
"""

import os
import pytest
from pathlib import Path
from typer.testing import CliRunner

from interlace.cli.main import app

runner = CliRunner()


class TestVersion:
    """Tests for --version flag."""

    def test_version_flag(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "interlace version" in result.output

    def test_version_short_flag(self):
        result = runner.invoke(app, ["-v"])
        assert result.exit_code == 0
        assert "interlace version" in result.output


class TestHelp:
    """Tests for help output."""

    def test_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "interlace" in result.output.lower()

    def test_no_args_shows_help(self):
        result = runner.invoke(app, [])
        assert result.exit_code == 0
        # Should show help text when no subcommand provided
        assert "interlace" in result.output.lower()

    def test_run_help(self):
        result = runner.invoke(app, ["run", "--help"])
        assert result.exit_code == 0
        assert "models" in result.output.lower() or "run" in result.output.lower()

    def test_init_help(self):
        result = runner.invoke(app, ["init", "--help"])
        assert result.exit_code == 0

    def test_info_help(self):
        result = runner.invoke(app, ["info", "--help"])
        assert result.exit_code == 0

    def test_config_help(self):
        result = runner.invoke(app, ["config", "--help"])
        assert result.exit_code == 0

    def test_schema_help(self):
        result = runner.invoke(app, ["schema", "--help"])
        assert result.exit_code == 0

    def test_plan_help(self):
        result = runner.invoke(app, ["plan", "--help"])
        assert result.exit_code == 0

    def test_migrate_help(self):
        result = runner.invoke(app, ["migrate", "--help"])
        assert result.exit_code == 0


class TestInit:
    """Tests for init command."""

    def test_init_creates_project(self, tmp_path):
        os.chdir(tmp_path)
        result = runner.invoke(app, ["init", "test-project"])
        assert result.exit_code == 0
        assert "Created Interlace project" in result.output

        project = tmp_path / "test-project"
        assert project.exists()
        assert (project / "config.yaml").exists()
        assert (project / "models").is_dir()
        assert (project / "tests").is_dir()
        assert (project / ".gitignore").exists()

    def test_init_default_name(self, tmp_path):
        os.chdir(tmp_path)
        result = runner.invoke(app, ["init"])
        assert result.exit_code == 0
        assert (tmp_path / "interlace-project").exists()

    def test_init_existing_dir_fails(self, tmp_path):
        os.chdir(tmp_path)
        (tmp_path / "existing").mkdir()
        result = runner.invoke(app, ["init", "existing"])
        assert result.exit_code == 1
        assert "already exists" in result.output


class TestRun:
    """Tests for run command."""

    def test_run_missing_config(self, tmp_path):
        """Run fails gracefully when no config.yaml exists."""
        result = runner.invoke(app, ["run", "--project-dir", str(tmp_path)])
        assert result.exit_code == 1

    def test_run_missing_models_dir(self, tmp_path):
        """Run fails when models/ dir doesn't exist."""
        (tmp_path / "config.yaml").write_text(
            "name: test\nconnections:\n  default:\n    type: duckdb\n    path: ':memory:'\n"
        )
        result = runner.invoke(app, ["run", "--project-dir", str(tmp_path)])
        assert result.exit_code == 1


class TestInfo:
    """Tests for info command."""

    def test_info_missing_config(self, tmp_path):
        """Info fails gracefully when no config.yaml exists."""
        result = runner.invoke(app, ["info", "--project-dir", str(tmp_path)])
        assert result.exit_code != 0
