"""Project configuration loaded from ``interlace.yaml``.

A small pydantic model with sensible defaults so a project works with no config
file at all. Paths are relative to the project root.

``${VAR}`` references anywhere in the YAML are substituted from the environment
before parsing (unset variables are left literal), so DSNs and secret values
never need to be committed: ``database: "ducklake:postgres:${WAREHOUSE_DSN}"``.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, ValidationError

from interlace.exceptions import ConfigurationError

CONFIG_FILE = "interlace.yaml"

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class SecretConfig(BaseModel):
    """A DuckDB ``CREATE SECRET`` issued at engine open (currently ``type: s3``) —
    how a DuckLake warehouse whose ``data_path`` is an object store authenticates.
    Values normally arrive via ``${VAR}`` interpolation."""

    type: str = "s3"
    key_id: str = ""
    secret: str = ""
    endpoint: str | None = None  # host[:port], no scheme; None => AWS default
    region: str | None = None
    url_style: str | None = None  # 'path' for MinIO/RustFS-style endpoints
    use_ssl: bool | None = None
    scope: str | None = None  # e.g. s3://bucket — pin the secret to one prefix


class ProjectConfig(BaseModel):
    """Top-level project settings."""

    name: str = "interlace"
    default_dialect: str = "duckdb"
    state_path: str = ".interlace/state.db"  # SQLite control-plane database
    # The warehouse. Default is DuckLake (Parquet data + SQL catalog) via DuckDB.
    # Also accepted: a DuckLake catalog hosted in a SQL database
    # ("ducklake:postgres:dbname=... host=..." — pair with data_path/metadata_schema),
    # a plain DuckDB file path, ":memory:", or "quack:<host>:<port>" to connect to a
    # warehouse served by `interlace serve --quack`.
    database: str = "ducklake:.interlace/warehouse.ducklake"
    # DuckLake attach options for non-default layouts: where the Parquet data lives
    # (local dir or s3://bucket/prefix/) and which schema of the catalog database
    # holds this warehouse's ducklake_* metadata (multiple warehouses can share one
    # catalog database, one schema each).
    data_path: str | None = None
    metadata_schema: str | None = None
    # Secrets to CREATE on the engine at open (name -> config), e.g. the S3
    # credential for an object-store data_path.
    secrets: dict[str, SecretConfig] = Field(default_factory=dict)
    quack_token: str | None = None  # token for quack: databases (or INTERLACE_QUACK_TOKEN)
    stream_path: str = ".interlace/streams.db"  # durable stream log (SQLite WAL)
    # Databases to ATTACH to the warehouse engine at open: alias -> DuckDB attach
    # URI/path (a .duckdb file, "postgres:...", "sqlite:...", ...). Models can read
    # them and table exports can write to them as <alias>.<schema>.<table>.
    attach: dict[str, str] = Field(default_factory=dict)
    model_paths: list[str] = Field(default_factory=lambda: ["models"])


def _interpolate_env(text: str) -> str:
    """Replace ``${VAR}`` with the environment value; unset vars stay literal (so a
    missing variable surfaces as an obvious ``${VAR}`` in errors, never silently '')."""
    return _ENV_REF.sub(lambda m: os.environ.get(m.group(1), m.group(0)), text)


def load_config(path: Path) -> ProjectConfig:
    """Load and validate config, returning defaults when the file is absent."""
    if not path.exists():
        return ProjectConfig()
    try:
        data = yaml.safe_load(_interpolate_env(path.read_text())) or {}
    except yaml.YAMLError as exc:
        raise ConfigurationError("could not parse config", details={"path": str(path), "error": str(exc)}) from exc
    if not isinstance(data, dict):
        raise ConfigurationError("config root must be a mapping", details={"path": str(path)})
    try:
        return ProjectConfig(**data)
    except ValidationError as exc:
        raise ConfigurationError("invalid config", details={"path": str(path), "error": str(exc)}) from exc
