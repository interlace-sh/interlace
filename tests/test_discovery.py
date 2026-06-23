"""Project discovery and loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from interlace.config.config import ProjectConfig
from interlace.dsl.discovery import discover_models
from interlace.project import Project

pytestmark = pytest.mark.unit


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def test_discovers_sql_and_python_models(tmp_path: Path) -> None:
    _write(tmp_path / "models" / "a.sql", "SELECT 1 AS x")
    _write(tmp_path / "models" / "silver" / "orders.sql", "SELECT 2 AS id")
    _write(
        tmp_path / "models" / "py_model.py",
        "from interlace import model\n\n@model(name='derived')\ndef build():\n    ...\n",
    )

    models = {m.name: m for m in discover_models(tmp_path, ["models"], "duckdb")}

    assert models["a"].sql == "SELECT 1 AS x"
    assert "silver.orders" in models  # path-derived name
    assert models["derived"].fn is not None  # python model registered via decorator


def test_underscore_python_files_are_skipped(tmp_path: Path) -> None:
    _write(tmp_path / "models" / "_helpers.py", "raise RuntimeError('should not be imported')")
    _write(tmp_path / "models" / "a.sql", "SELECT 1 AS x")

    models = discover_models(tmp_path, ["models"], "duckdb")
    assert [m.name for m in models] == ["a"]


def test_missing_model_path_is_ignored(tmp_path: Path) -> None:
    assert discover_models(tmp_path, ["models"], "duckdb") == []


def test_project_load_uses_defaults_without_config(tmp_path: Path) -> None:
    _write(tmp_path / "models" / "a.sql", "SELECT 1 AS x")
    project = Project.load(tmp_path)

    assert project.config == ProjectConfig()  # defaults
    assert [m.name for m in project.models] == ["a"]
    assert "a" in project.compile().models


def test_project_load_reads_config(tmp_path: Path) -> None:
    _write(tmp_path / "interlace.yaml", "name: shop\ndefault_dialect: snowflake\n")
    _write(tmp_path / "models" / "a.sql", "SELECT 1 AS x")

    project = Project.load(tmp_path)
    assert project.config.name == "shop"
    assert project.config.default_dialect == "snowflake"
