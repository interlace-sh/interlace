"""Per-model config blocks in SQL files."""

from __future__ import annotations

from pathlib import Path

import pytest

from interlace.dsl.discovery import discover_models
from interlace.dsl.sql_config import extract_sql_config
from interlace.exceptions import ConfigurationError, DefinitionError

pytestmark = pytest.mark.unit


def test_extracts_config_and_strips_block() -> None:
    config, sql = extract_sql_config("/*\ninterlace:\n  materialise: view\n  key: id\n*/\nSELECT 1 AS id")
    assert config == {"materialise": "view", "key": "id"}
    assert sql == "SELECT 1 AS id"


def test_no_block_returns_content_unchanged() -> None:
    config, sql = extract_sql_config("SELECT 1 AS x")
    assert config == {}
    assert sql == "SELECT 1 AS x"


def test_block_without_interlace_key_is_not_config() -> None:
    content = "/* just a comment */\nSELECT 1 AS x"
    config, sql = extract_sql_config(content)
    assert config == {}
    assert sql == content  # not stripped


def test_malformed_yaml_is_ignored() -> None:
    content = "/* interlace: : : */\nSELECT 1"
    config, sql = extract_sql_config(content)
    assert config == {}


def test_non_mapping_config_raises() -> None:
    with pytest.raises(ConfigurationError):
        extract_sql_config("/*\ninterlace:\n  - not\n  - a\n  - map\n*/\nSELECT 1")


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def test_discovery_applies_sql_config(tmp_path: Path) -> None:
    _write(tmp_path / "models" / "v.sql", "/*\ninterlace:\n  materialise: view\n  tags: [a, b]\n*/\nSELECT 1 AS x")
    models = {m.name: m for m in discover_models(tmp_path, ["models"], "duckdb")}

    assert models["v"].materialise == "view"
    assert models["v"].tags == ("a", "b")


def test_discovery_rejects_unknown_materialise(tmp_path: Path) -> None:
    _write(tmp_path / "models" / "bad.sql", "/*\ninterlace:\n  materialise: nonsense\n*/\nSELECT 1 AS x")
    with pytest.raises(DefinitionError):
        discover_models(tmp_path, ["models"], "duckdb")
