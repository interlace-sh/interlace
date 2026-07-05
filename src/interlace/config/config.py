"""Project configuration loaded from ``interlace.yaml``.

A small pydantic model with sensible defaults so a project works with no config
file at all. Paths are relative to the project root.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, ValidationError

from interlace.exceptions import ConfigurationError

CONFIG_FILE = "interlace.yaml"


class ProjectConfig(BaseModel):
    """Top-level project settings."""

    name: str = "interlace"
    default_dialect: str = "duckdb"
    state_path: str = ".interlace/state.db"  # SQLite control-plane database
    # The warehouse. Default is DuckLake (Parquet data + SQL catalog) via DuckDB.
    # Also accepted: a plain DuckDB file path, ":memory:", or "quack:<host>:<port>"
    # to connect to a warehouse served by `interlace serve --quack`.
    database: str = "ducklake:.interlace/warehouse.ducklake"
    quack_token: str | None = None  # token for quack: databases (or INTERLACE_QUACK_TOKEN)
    stream_path: str = ".interlace/streams.db"  # durable stream log (SQLite WAL)
    model_paths: list[str] = Field(default_factory=lambda: ["models"])


def load_config(path: Path) -> ProjectConfig:
    """Load and validate config, returning defaults when the file is absent."""
    if not path.exists():
        return ProjectConfig()
    try:
        data = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise ConfigurationError("could not parse config", details={"path": str(path), "error": str(exc)}) from exc
    if not isinstance(data, dict):
        raise ConfigurationError("config root must be a mapping", details={"path": str(path)})
    try:
        return ProjectConfig(**data)
    except ValidationError as exc:
        raise ConfigurationError("invalid config", details={"path": str(path), "error": str(exc)}) from exc
