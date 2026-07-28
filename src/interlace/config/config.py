"""Project configuration loaded from ``interlace.yaml``.

A small pydantic model with sensible defaults so a project works with no config
file at all. Paths are relative to the project root.

``${VAR}`` references anywhere in the YAML are substituted before parsing, so
DSNs and secret values never need to be committed:
``database: "ducklake:postgres:${WAREHOUSE_DSN}"``. Values come from the real
environment first, then from a ``.env`` file next to the config (dotenv
KEY=VALUE lines; the process environment always wins). Unset variables are
left literal so a missing one surfaces as an obvious ``${VAR}`` in errors.
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
_ENV_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


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


_ENGINE_TYPES = frozenset({"duckdb", "ducklake", "quack"})
_TYPE_DIALECT = {
    "duckdb": "duckdb",
    "ducklake": "duckdb",
    "quack": "duckdb",
    "postgres": "postgres",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
}


class EngineConfig(BaseModel):
    """One named execution engine (warehouse gateway).

    DuckDB-family types (``duckdb`` / ``ducklake`` / ``quack``) are fully supported.
    Additional types are reserved for remote adapters (Postgres, Snowflake, …);
    declaring them fails at open until an adapter ships.
    """

    type: str = "ducklake"
    # Path / URI for DuckDB-family engines. Also accepted on the project top level
    # as ``database:`` (synthesised into the ``default`` engine).
    database: str | None = None
    # The catalog's ATTACH alias. Defaults to the engine name, or the project name
    # for the ``default`` engine. Set it when a SCHEMA inside the warehouse would
    # otherwise share the alias — DuckDB cannot bind ``x.y`` when ``x`` is both a
    # catalog and a schema, so a project named `seccl` holding a `seccl` schema
    # needs one of the two renamed.
    alias: str | None = None
    data_path: str | None = None
    metadata_schema: str | None = None
    secrets: dict[str, SecretConfig] = Field(default_factory=dict)
    quack_token: str | None = None
    attach: dict[str, str] = Field(default_factory=dict)
    dialect: str | None = None  # defaults from type (duckdb for DuckDB-family)

    def resolved_dialect(self) -> str:
        if self.dialect:
            return self.dialect
        return _TYPE_DIALECT.get(self.type, "duckdb")


class ProjectConfig(BaseModel):
    """Top-level project settings."""

    name: str = "interlace"
    default_dialect: str = "duckdb"
    # Named engines (see ``engines``). Single-engine projects leave this as
    # ``default`` and use the top-level ``database`` / ``attach`` fields.
    default_engine: str = "default"
    engines: dict[str, EngineConfig] = Field(default_factory=dict)
    state_path: str = ".interlace/state.db"  # SQLite control-plane database
    # The warehouse. Default is DuckLake (Parquet data + SQL catalog) via DuckDB.
    # Also accepted: a DuckLake catalog hosted in a SQL database
    # ("ducklake:postgres:dbname=... host=..." — pair with data_path/metadata_schema),
    # a plain DuckDB file path, ":memory:", or "quack:<host>:<port>" to connect to a
    # warehouse served by `interlace serve --quack`.
    # When ``engines.default`` is not set, these top-level fields synthesise it.
    database: str = "ducklake:.interlace/warehouse.ducklake"
    # The warehouse catalog's ATTACH alias (defaults to ``name``). Set it when a
    # schema inside the warehouse shares the project name — see EngineConfig.alias.
    alias: str | None = None
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
    # This is T0 federation (all SQL still runs in DuckDB) — see docs/architecture/MULTI_ENGINE.md.
    attach: dict[str, str] = Field(default_factory=dict)
    model_paths: list[str] = Field(default_factory=lambda: ["models"])

    def engine_configs(self) -> dict[str, EngineConfig]:
        """Resolved engine map: explicit ``engines`` plus a synthesised ``default``
        from the top-level warehouse fields when that name is not already set."""
        result = dict(self.engines)
        if "default" not in result:
            result["default"] = _default_engine_from_top_level(self)
        return result


def _infer_engine_type(database: str) -> str:
    if database.startswith("quack:"):
        return "quack"
    if database.startswith("ducklake:"):
        return "ducklake"
    return "duckdb"


def _default_engine_from_top_level(config: ProjectConfig) -> EngineConfig:
    return EngineConfig(
        type=_infer_engine_type(config.database),
        database=config.database,
        alias=config.alias,
        data_path=config.data_path,
        metadata_schema=config.metadata_schema,
        secrets=config.secrets,
        quack_token=config.quack_token,
        attach=config.attach,
        dialect=config.default_dialect if config.default_dialect != "duckdb" else None,
    )


def _interpolate_env(text: str, fallback: dict[str, str] | None = None) -> str:
    """Replace ``${VAR}`` from the process environment, then the ``.env`` fallback;
    unset vars stay literal (so a missing variable surfaces as an obvious ``${VAR}``
    in errors, never silently '')."""
    extra = fallback or {}

    def resolve(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in os.environ:
            return os.environ[name]
        return extra.get(name, match.group(0))

    return _ENV_REF.sub(resolve, text)


def load_dotenv(path: Path) -> dict[str, str]:
    """KEY=VALUE pairs from a ``.env`` file (missing file = empty). Supports the
    common dotenv subset: comments and blank lines skipped, optional ``export``
    prefix, optional single/double quotes around the value. Never mutates the
    process environment — values only feed ``${VAR}`` interpolation."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, raw = stripped.removeprefix("export ").partition("=")
        key = key.strip()
        if not _ENV_KEY.fullmatch(key):
            continue
        value = raw.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def load_config(path: Path) -> ProjectConfig:
    """Load and validate config, returning defaults when the file is absent.

    ``${VAR}`` references resolve from the process environment first, then from
    a ``.env`` file sitting next to the config."""
    if not path.exists():
        return ProjectConfig()
    dotenv = load_dotenv(path.parent / ".env")
    try:
        data = yaml.safe_load(_interpolate_env(path.read_text(), dotenv)) or {}
    except yaml.YAMLError as exc:
        raise ConfigurationError("could not parse config", details={"path": str(path), "error": str(exc)}) from exc
    if not isinstance(data, dict):
        raise ConfigurationError("config root must be a mapping", details={"path": str(path)})
    try:
        return ProjectConfig(**data)
    except ValidationError as exc:
        raise ConfigurationError("invalid config", details={"path": str(path), "error": str(exc)}) from exc
