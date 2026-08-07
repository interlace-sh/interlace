"""Project discovery and loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from interlace.config.config import ProjectConfig
from interlace.dsl.discovery import discover_models
from interlace.exceptions import DefinitionError
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


def test_sql_header_backfill_is_honoured(tmp_path: Path) -> None:
    """`backfill:` in a SQL header must reach the ModelDef — it only matters for
    incremental, which is SQL-only, so if the header ignored it the config
    would be unreachable."""
    _write(
        tmp_path / "models" / "events.sql",
        "/* interlace: {strategy: incremental, time_column: day, interval: 1d, backfill: none} */\n"
        "SELECT day FROM raw",
    )
    models = {m.name: m for m in discover_models(tmp_path, ["models"], "duckdb")}
    assert models["events"].backfill == "none"


def test_underscore_python_files_are_skipped(tmp_path: Path) -> None:
    _write(tmp_path / "models" / "_helpers.py", "raise RuntimeError('should not be imported')")
    _write(tmp_path / "models" / "a.sql", "SELECT 1 AS x")

    models = discover_models(tmp_path, ["models"], "duckdb")
    assert [m.name for m in models] == ["a"]


def test_missing_model_path_is_ignored(tmp_path: Path) -> None:
    assert discover_models(tmp_path, ["models"], "duckdb") == []


def test_broken_model_file_raises_a_clean_error_naming_the_file(tmp_path: Path) -> None:
    # A typo/import error in user model code must read like a user error — one line
    # naming the file — not a raw traceback through interlace's import machinery.
    _write(tmp_path / "models" / "orders.py", "from interlace import mdoel  # typo\n")
    with pytest.raises(DefinitionError) as excinfo:
        discover_models(tmp_path, ["models"], "duckdb")
    message = excinfo.value.message
    assert "orders.py" in message and "could not load" in message  # names the file
    assert "ImportError" in message  # and the underlying cause
    assert isinstance(excinfo.value.__cause__, ImportError)  # original preserved for --debug


def test_bad_model_config_error_is_not_rewrapped(tmp_path: Path) -> None:
    # A clean DefinitionError from the @model decorator must pass through unchanged,
    # not get buried under a generic "could not load".
    _write(
        tmp_path / "models" / "bad.py",
        "from interlace import model\n\n@model(materialise='nope')\ndef bad():\n    ...\n",
    )
    with pytest.raises(DefinitionError) as excinfo:
        discover_models(tmp_path, ["models"], "duckdb")
    assert "could not load" not in excinfo.value.message
    assert "materialise" in excinfo.value.message


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
