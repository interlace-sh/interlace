"""
Tests for configuration loading and resolution.
"""

import os
import pytest
from pathlib import Path

from interlace.config.loader import load_config, Config, _merge_dict


class TestConfig:
    """Tests for Config class."""

    def test_basic_access(self):
        cfg = Config({"name": "test", "connections": {"default": {"type": "duckdb"}}})
        assert cfg.get("name") == "test"
        assert cfg.data["name"] == "test"

    def test_dot_notation(self):
        cfg = Config({"connections": {"default": {"type": "duckdb"}}})
        assert cfg.get("connections.default.type") == "duckdb"

    def test_dot_notation_missing_returns_default(self):
        cfg = Config({"a": 1})
        assert cfg.get("a.b.c", "fallback") == "fallback"

    def test_contains(self):
        cfg = Config({"a": {"b": 1}})
        assert "a" in cfg
        assert "a.b" in cfg
        assert "a.c" not in cfg
        assert "z" not in cfg

    def test_getitem(self):
        cfg = Config({"name": "test", "nested": {"key": "val"}})
        assert cfg["name"] == "test"
        # Nested dict access returns Config
        nested = cfg["nested"]
        assert isinstance(nested, Config)
        assert nested["key"] == "val"

    def test_getitem_dot_notation(self):
        cfg = Config({"a": {"b": "c"}})
        assert cfg["a.b"] == "c"

    def test_getitem_missing_raises(self):
        cfg = Config({"a": 1})
        with pytest.raises(KeyError):
            _ = cfg["missing"]

    def test_iter_keys_values_items(self):
        data = {"a": 1, "b": 2}
        cfg = Config(data)
        assert set(cfg.keys()) == {"a", "b"}
        assert list(cfg) == list(data.keys())
        assert set(cfg.values()) == {1, 2}
        assert set(cfg.items()) == {("a", 1), ("b", 2)}

    def test_validate_valid(self):
        cfg = Config({"connections": {"default": {"type": "duckdb"}}})
        cfg.validate()  # Should not raise

    def test_validate_non_dict_raises(self):
        cfg = Config.__new__(Config)
        cfg.data = "not a dict"
        with pytest.raises(ValueError, match="must be a dictionary"):
            cfg.validate()

    def test_validate_connections_must_be_dict(self):
        cfg = Config({"connections": "bad"})
        with pytest.raises(ValueError, match="must be a dictionary"):
            cfg.validate()


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_basic_config(self, tmp_path):
        (tmp_path / "config.yaml").write_text(
            "name: testproj\nconnections:\n  default:\n    type: duckdb\n"
        )
        cfg = load_config(tmp_path)
        assert cfg.get("name") == "testproj"

    def test_missing_config_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="config.yaml"):
            load_config(tmp_path)

    def test_env_overlay(self, tmp_path):
        (tmp_path / "config.yaml").write_text("name: base\nlevel: 1\n")
        (tmp_path / "config.prod.yaml").write_text("level: 2\nextra: true\n")
        cfg = load_config(tmp_path, env="prod")
        assert cfg.get("name") == "base"  # from base
        assert cfg.get("level") == 2  # overridden
        assert cfg.get("extra") is True  # added

    def test_empty_config_returns_empty_dict(self, tmp_path):
        (tmp_path / "config.yaml").write_text("")
        cfg = load_config(tmp_path)
        assert cfg.data == {}

    def test_invalid_yaml_raises(self, tmp_path):
        (tmp_path / "config.yaml").write_text(":\n  :\n  invalid: [")
        with pytest.raises(ValueError, match="Error parsing"):
            load_config(tmp_path)

    def test_env_var_substitution(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MY_DB_PATH", "/data/test.duckdb")
        (tmp_path / "config.yaml").write_text(
            "connections:\n  default:\n    type: duckdb\n    path: ${MY_DB_PATH}\n"
        )
        cfg = load_config(tmp_path, env="dev")
        conn = cfg.get("connections.default.path")
        assert conn == "/data/test.duckdb"

    def test_env_placeholder_substitution(self, tmp_path):
        (tmp_path / "config.yaml").write_text(
            "connections:\n  default:\n    type: duckdb\n    path: data/{env}/main.duckdb\n"
        )
        cfg = load_config(tmp_path, env="staging")
        conn = cfg.get("connections.default.path")
        assert "staging" in conn


class TestMergeDict:
    """Tests for _merge_dict helper."""

    def test_simple_override(self):
        base = {"a": 1, "b": 2}
        override = {"b": 3}
        _merge_dict(base, override)
        assert base == {"a": 1, "b": 3}

    def test_nested_merge(self):
        base = {"a": {"x": 1, "y": 2}}
        override = {"a": {"y": 3, "z": 4}}
        _merge_dict(base, override)
        assert base == {"a": {"x": 1, "y": 3, "z": 4}}

    def test_add_new_keys(self):
        base = {"a": 1}
        override = {"b": 2}
        _merge_dict(base, override)
        assert base == {"a": 1, "b": 2}

    def test_replace_non_dict_with_dict(self):
        base = {"a": "string"}
        override = {"a": {"nested": True}}
        _merge_dict(base, override)
        assert base == {"a": {"nested": True}}
