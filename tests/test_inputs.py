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


def test_watched_input_bytes_change_the_fingerprint(tmp_path: Path) -> None:
    (tmp_path / "events.csv").write_text("id\n1\n")
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orders.sql").write_text("SELECT id FROM events")
    (tmp_path / "models" / "constant.sql").write_text("SELECT 1 AS n")
    (tmp_path / "interlace.yaml").write_text(
        "inputs:\n  events:\n    format: csv\n    path: events.csv\n    watch: true\n"
    )
    first = Project.load(tmp_path).compile()
    orders = first.models["orders"].fingerprint
    constant = first.models["constant"].fingerprint
    (tmp_path / "events.csv").write_text("id\n2\n")
    second = Project.load(tmp_path).compile()
    assert second.models["orders"].fingerprint != orders
    assert second.models["constant"].fingerprint == constant


def test_unwatched_input_bytes_do_not_change_the_fingerprint(tmp_path: Path) -> None:
    (tmp_path / "events.csv").write_text("id\n1\n")
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orders.sql").write_text("SELECT id FROM events")
    (tmp_path / "interlace.yaml").write_text("inputs:\n  events:\n    format: csv\n    path: events.csv\n")
    first = Project.load(tmp_path).compile().models["orders"].fingerprint
    (tmp_path / "events.csv").write_text("id\n2\n")
    assert Project.load(tmp_path).compile().models["orders"].fingerprint == first


def test_watched_input_must_exist_and_be_local(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orders.sql").write_text("SELECT id FROM events")
    (tmp_path / "interlace.yaml").write_text(
        "inputs:\n  events:\n    format: csv\n    path: missing.csv\n    watch: true\n"
    )
    with pytest.raises(ConfigurationError, match="matched no files"):
        Project.load(tmp_path).compile()
    (tmp_path / "interlace.yaml").write_text(
        "inputs:\n  events:\n    format: parquet\n    path: s3://bucket/events.parquet\n    watch: true\n"
    )
    with pytest.raises(ConfigurationError, match="not a local path"):
        Project.load(tmp_path).compile()


def test_watched_glob_includes_every_matching_file(tmp_path: Path) -> None:
    folder = tmp_path / "data"
    folder.mkdir()
    (folder / "a.csv").write_text("id\n1\n")
    (folder / "b.csv").write_text("id\n2\n")
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orders.sql").write_text("SELECT id FROM events")
    (tmp_path / "interlace.yaml").write_text(
        "inputs:\n  events:\n    format: csv\n    path: data/*.csv\n    watch: true\n"
    )
    first = Project.load(tmp_path).compile().models["orders"].fingerprint
    (folder / "b.csv").write_text("id\n3\n")
    assert Project.load(tmp_path).compile().models["orders"].fingerprint != first


def test_input_on_a_non_duckdb_engine_is_rejected(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orders.sql").write_text("/* interlace: {engine: pg} */\nSELECT id FROM events")
    (tmp_path / "interlace.yaml").write_text(
        "engines:\n  pg: {type: postgres, database: 'postgresql://u@db.internal:5432/app'}\n"
        "inputs:\n  events: {format: csv, path: events.csv}\n"
    )
    with pytest.raises(CompilationError, match="DuckDB"):
        Project.load(tmp_path).compile()
