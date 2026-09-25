"""DuckDB external inputs become views a model can FROM."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from interlace.exceptions import CompilationError, ConfigurationError
from interlace.project import Project

pytestmark = pytest.mark.unit


def test_csv_input_is_a_relation(tmp_path: Path) -> None:
    day = datetime.now(UTC).strftime("%Y-%m-%d")
    folder = tmp_path / day
    folder.mkdir()
    (folder / "events.csv").write_text("id,name\n1,a\n")
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orders.sql").write_text("SELECT id, name FROM events")
    (tmp_path / "interlace.yaml").write_text("inputs:\n  events:\n    format: csv\n    path: ${date}/events.csv\n")
    project = Project.load(tmp_path)
    engine = project.open_engine()
    try:
        reader = engine.fetch_sync("SELECT id, name FROM events")
        assert reader.read_all().to_pylist() == [{"id": 1, "name": "a"}]
    finally:
        engine.close()


def test_missing_extension_is_a_clear_error() -> None:
    from interlace.engines.duckdb import DuckDBAdapter
    from interlace.inputs import _load_extension

    engine = DuckDBAdapter.in_memory()
    try:
        with pytest.raises(ConfigurationError, match="not_a_real_extension"):
            _load_extension(engine, "not_a_real_extension", input_name="events")
    finally:
        engine.close()


def test_input_on_a_non_duckdb_engine_is_rejected(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orders.sql").write_text("/* interlace: {engine: pg} */\nSELECT id FROM events")
    (tmp_path / "interlace.yaml").write_text(
        "engines:\n  pg: {type: postgres, database: 'postgresql://u@db.internal:5432/app'}\n"
        "inputs:\n  events: {format: csv, path: events.csv}\n"
    )
    with pytest.raises(CompilationError, match="DuckDB"):
        Project.load(tmp_path).compile()
