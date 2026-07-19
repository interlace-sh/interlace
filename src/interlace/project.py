"""A loaded project: config + discovered models, with engine/state factories.

This is the entry point the CLI builds on — ``Project.load(dir)`` reads the
config, discovers models, and can compile them and open the warehouse engine and
control-plane state store at the configured (root-relative) paths.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from interlace.config.config import CONFIG_FILE, ProjectConfig, SecretConfig, load_config
from interlace.dsl.decorators import REGISTRY, CheckDef, ModelDef, StreamDef
from interlace.dsl.discovery import discover_models
from interlace.engines.duckdb import DuckDBAdapter
from interlace.graph.project import CompiledProject, compile_models
from interlace.state.store import SqliteStateStore
from interlace.streaming.log import SqliteStreamLog


def _secret_sql(name: str, secret: SecretConfig) -> str:
    """Render one config secret as a DuckDB ``CREATE SECRET`` statement. Values are
    single-quote-escaped; unknown/blank optional fields are simply omitted."""

    def q(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    parts = [f"TYPE {secret.type}", f"KEY_ID {q(secret.key_id)}", f"SECRET {q(secret.secret)}"]
    if secret.endpoint:
        parts.append(f"ENDPOINT {q(secret.endpoint)}")
    if secret.region:
        parts.append(f"REGION {q(secret.region)}")
    if secret.url_style:
        parts.append(f"URL_STYLE {q(secret.url_style)}")
    if secret.use_ssl is not None:
        parts.append(f"USE_SSL {'true' if secret.use_ssl else 'false'}")
    if secret.scope:
        parts.append(f"SCOPE {q(secret.scope)}")
    from sqlglot import exp

    return f"CREATE OR REPLACE SECRET {exp.to_identifier(name).sql('duckdb')} ({', '.join(parts)})"


@dataclass
class Project:
    root: Path
    config: ProjectConfig
    models: list[ModelDef]
    checks: list[CheckDef]
    streams: list[StreamDef]

    @classmethod
    def load(cls, root: Path | str) -> Project:
        root = Path(root)
        config = load_config(root / CONFIG_FILE)
        models = discover_models(root, config.model_paths, config.default_dialect)
        return cls(
            root=root,
            config=config,
            models=models,
            checks=list(REGISTRY.checks),
            streams=list(REGISTRY.streams.values()),
        )

    def compile(self) -> CompiledProject:
        return compile_models(self.models, default_dialect=self.config.default_dialect, checks=self.checks)

    def open_engine(self) -> DuckDBAdapter:
        """Open the warehouse: DuckLake (default; local file catalog, or a catalog hosted
        in a SQL database with the data on a filesystem/object store), a plain DuckDB
        file, ":memory:", or a remote warehouse served over the quack protocol."""
        database = self.config.database
        if database.startswith("quack:"):
            from interlace.engines.quack import QuackAdapter  # lazy: only quack clients need it

            token = self.config.quack_token or os.environ.get("INTERLACE_QUACK_TOKEN")
            return QuackAdapter.connect(database, token=token)
        if database.startswith("ducklake:"):
            catalog = database.removeprefix("ducklake:")
            # Remote catalogs ("postgres:dbname=…", "mysql:…", "sqlite:…") are DSNs,
            # not paths — never filesystem-resolve them.
            remote_catalog = catalog.startswith(("postgres:", "mysql:", "sqlite:"))
            if not remote_catalog and not Path(catalog).is_absolute():
                resolved = self.root / catalog
                resolved.parent.mkdir(parents=True, exist_ok=True)
                database = f"ducklake:{resolved}"
            if remote_catalog or self.config.data_path or self.config.metadata_schema or self.config.secrets:
                engine = self._open_ducklake_with_options(database, remote_catalog=remote_catalog)
            else:
                engine = DuckDBAdapter.connect(database)
        else:
            if database != ":memory:":
                path = self.root / database
                path.parent.mkdir(parents=True, exist_ok=True)
                database = str(path)
            engine = DuckDBAdapter.connect(database)
        for alias, uri in self.config.attach.items():  # reads + table exports reach these
            target = uri
            if "://" not in uri and ":" not in uri.split("/")[0] and not Path(uri).is_absolute():
                target = str(self.root / uri)  # bare relative path: resolve against the project
            engine.attach(alias, target)
        return engine

    def _open_ducklake_with_options(self, database: str, *, remote_catalog: bool) -> DuckDBAdapter:
        """DuckLake with attach options/credentials: explicit ATTACH (DATA_PATH /
        METADATA_SCHEMA) after installing the needed extensions and creating the
        configured secrets — e.g. a Postgres-hosted catalog with Parquet on S3."""
        data_path = self.config.data_path
        if data_path and "://" not in data_path and not Path(data_path).is_absolute():
            resolved_data = self.root / data_path
            resolved_data.mkdir(parents=True, exist_ok=True)
            data_path = str(resolved_data)
        extensions = ["ducklake"]
        if database.removeprefix("ducklake:").startswith("postgres:"):
            extensions.append("postgres")
        if (data_path or "").startswith(("s3://", "gcs://", "r2://")) or any(
            s.type == "s3" for s in self.config.secrets.values()
        ):
            extensions.append("httpfs")
        secrets = [_secret_sql(name, secret) for name, secret in self.config.secrets.items()]
        return DuckDBAdapter.connect_ducklake(
            database,
            alias=self.config.name or "warehouse",
            data_path=data_path,
            metadata_schema=self.config.metadata_schema,
            secrets=secrets,
            extensions=extensions,
        )

    async def open_state(self) -> SqliteStateStore:
        path = self.root / self.config.state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        return await SqliteStateStore.open(path)

    async def open_stream_log(self) -> SqliteStreamLog:
        path = self.root / self.config.stream_path
        path.parent.mkdir(parents=True, exist_ok=True)
        return await SqliteStreamLog.open(path)
