"""A loaded project: config + discovered models, with engine/state factories.

This is the entry point the CLI builds on — ``Project.load(dir)`` reads the
config, discovers models, and can compile them and open the warehouse engine(s)
and control-plane state store at the configured (root-relative) paths.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from interlace.config.config import CONFIG_FILE, EngineConfig, ProjectConfig, SecretConfig, load_config
from interlace.dsl.decorators import REGISTRY, CheckDef, ModelDef, StreamDef
from interlace.dsl.discovery import discover_models
from interlace.engines.base import EngineAdapter
from interlace.engines.duckdb import DuckDBAdapter
from interlace.engines.registry import EngineRegistry
from interlace.exceptions import ConfigurationError
from interlace.graph.project import CompiledProject, compile_models
from interlace.state.store import SqliteStateStore
from interlace.streaming.log import SqliteStreamLog

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_PG_HOST = re.compile(r"\b(host|hostaddr|service)\s*=")


def _require_explicit_pg_host(dsn: str, context: str) -> None:
    """Fail fast when a Postgres DSN names no host: libpq would silently fall back
    to its defaults — the local socket / localhost:5432 — i.e. whichever Postgres
    happens to live on this machine. A pipeline that writes must name its target.

    Every deliberate libpq form still passes: ``host=``/``hostaddr=`` (including
    unix sockets), URI hosts, URI ``?host=`` query params, ``service=`` (the host
    lives in pg_service.conf), and deployments that set PGHOST/PGSERVICE."""
    from urllib.parse import parse_qs, urlparse

    bare = dsn if dsn.startswith(("postgres://", "postgresql://")) else dsn.removeprefix("postgres:")
    if bare.startswith(("postgresql://", "postgres://")):
        parsed = urlparse(bare)
        query = parse_qs(parsed.query)
        if parsed.hostname or "host" in query or "hostaddr" in query or "service" in query:
            return
    elif _PG_HOST.search(bare):
        return
    if os.environ.get("PGHOST") or os.environ.get("PGSERVICE"):
        return  # the environment names the target via libpq's own convention
    raise ConfigurationError(
        f"{context}: the Postgres DSN names no host, so it would silently connect to whatever "
        f"Postgres lives on libpq's default (local socket / localhost:5432). Name the target: "
        f"add host= and port= (or service=, or a postgresql://user@host:port/dbname URI, "
        f"or set PGHOST).",
        details={"context": context},  # never echo the DSN: it may carry credentials
    )


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
        engine_cfgs = self.config.engine_configs()
        return compile_models(
            self.models,
            default_dialect=self.config.default_dialect,
            default_engine=self.config.default_engine,
            engine_dialects={name: cfg.resolved_dialect() for name, cfg in engine_cfgs.items()},
            known_engines=set(engine_cfgs),
            checks=self.checks,
        )

    def open_engine(self, name: str | None = None) -> EngineAdapter:
        """Open one engine (default: ``config.default_engine``). Prefer
        :meth:`open_engines` when applying multi-engine projects."""
        engine_name = name or self.config.default_engine
        configs = self.config.engine_configs()
        if engine_name not in configs:
            raise ConfigurationError(
                f"unknown engine {engine_name!r}",
                details={"engines": sorted(configs)},
            )
        return self._open_engine_config(engine_name, configs[engine_name])

    def open_engines(self) -> EngineRegistry:
        """Lazy registry of every configured engine. Call ``close()`` when done."""
        configs = self.config.engine_configs()
        if self.config.default_engine not in configs:
            raise ConfigurationError(
                f"default_engine {self.config.default_engine!r} is not a configured engine",
                details={"engines": sorted(configs)},
            )

        def opener(engine_name: str) -> EngineAdapter:
            return self._open_engine_config(engine_name, configs[engine_name])

        return EngineRegistry(
            configs.keys(),
            opener,
            default=self.config.default_engine,
            attach_uris=self._attachable_uris(configs),
        )

    def _attachable_uris(self, configs: dict[str, EngineConfig]) -> dict[str, str]:
        """URIs a DuckDB-family target could ATTACH to read an engine directly
        (the transfer fast lane). In-memory and quack engines are not attachable."""
        uris: dict[str, str] = {}
        for name, cfg in configs.items():
            database = cfg.database or ""
            if cfg.type in ("duckdb", "ducklake") and database and database != ":memory:":
                target = database
                path_part = target.removeprefix("ducklake:")
                if path_part != ":memory:" and not Path(path_part).is_absolute():
                    resolved = str(self.root / path_part)
                    target = f"ducklake:{resolved}" if target.startswith("ducklake:") else resolved
                uris[name] = target
            elif cfg.type == "postgres" and database:
                # the fast lane ATTACHes this DSN without ever opening the adapter,
                # so the no-silent-localhost guard must run here too
                _require_explicit_pg_host(database, f"engine {name!r} (attach fast lane)")
                uris[name] = database  # DuckDB's postgres extension attaches DSNs/URIs
        return uris

    def _open_engine_config(self, name: str, cfg: EngineConfig) -> EngineAdapter:
        """Open a single engine from its config (duckdb / ducklake / quack / postgres)."""
        self._reject_unresolved_env(cfg)
        if cfg.type == "postgres":
            from interlace.engines.postgres import PostgresAdapter  # lazy: needs the adbc extra

            if not cfg.database:
                raise ConfigurationError(
                    f"engine {name!r}: postgres needs a DSN in 'database' (postgresql://...)",
                    details={"engine": name},
                )
            _require_explicit_pg_host(cfg.database, f"engine {name!r}")
            return PostgresAdapter.connect(cfg.database)
        if cfg.type not in ("duckdb", "ducklake", "quack"):
            raise ConfigurationError(
                f"engine {name!r}: type {cfg.type!r} is not implemented yet "
                f"(supported: duckdb, ducklake, quack, postgres). See docs/architecture/MULTI_ENGINE.md",
                details={"engine": name, "type": cfg.type},
            )
        database = cfg.database or "ducklake:.interlace/warehouse.ducklake"
        if cfg.type == "quack" or database.startswith("quack:"):
            from interlace.engines.quack import QuackAdapter

            token = cfg.quack_token or os.environ.get("INTERLACE_QUACK_TOKEN")
            return QuackAdapter.connect(database, token=token)

        if cfg.type == "ducklake" or database.startswith("ducklake:"):
            catalog = database.removeprefix("ducklake:")
            remote_catalog = catalog.startswith(("postgres:", "mysql:", "sqlite:"))
            if catalog.startswith("postgres:"):
                _require_explicit_pg_host(catalog, f"engine {name!r} (ducklake catalog)")
            if not remote_catalog and not Path(catalog).is_absolute():
                resolved = self.root / catalog
                resolved.parent.mkdir(parents=True, exist_ok=True)
                database = f"ducklake:{resolved}"
            if remote_catalog or cfg.data_path or cfg.metadata_schema or cfg.secrets:
                engine = self._open_ducklake_with_options(name, database, cfg, remote_catalog=remote_catalog)
            else:
                engine = DuckDBAdapter.connect(database)
        else:
            if database != ":memory:":
                path = Path(database) if Path(database).is_absolute() else self.root / database
                path.parent.mkdir(parents=True, exist_ok=True)
                database = str(path)
            engine = DuckDBAdapter.connect(database)

        for alias, uri in cfg.attach.items():
            target = uri
            if uri.startswith(("postgres:", "postgres://", "postgresql://")):
                _require_explicit_pg_host(uri, f"attach {alias!r}")
            elif "://" not in uri and ":" not in uri.split("/")[0] and not Path(uri).is_absolute():
                target = str(self.root / uri)
            engine.attach(alias, target)
        return engine

    def _reject_unresolved_env(self, cfg: EngineConfig | None = None) -> None:
        """Fail fast when ``${VAR}`` survived config interpolation (the variable is
        unset). Left alone, DuckDB treats the literal ``${VAR}`` as a PATH — creating
        a directory of that name — before failing somewhere far less obvious."""
        refs: set[str] = set()
        if cfg is None:
            candidates = [self.config.database, self.config.data_path or ""]
            secrets = self.config.secrets
        else:
            candidates = [cfg.database or "", cfg.data_path or ""]
            secrets = cfg.secrets
        for secret in secrets.values():
            candidates += [secret.key_id, secret.secret, secret.endpoint or ""]
        for value in candidates:
            refs.update(_ENV_REF.findall(value))
        if refs:
            raise ConfigurationError(
                "unresolved ${VAR} in warehouse config — set the environment variable(s)",
                details={"variables": sorted(refs)},
            )

    def _open_ducklake_with_options(
        self, name: str, database: str, cfg: EngineConfig, *, remote_catalog: bool
    ) -> DuckDBAdapter:
        """DuckLake with attach options/credentials: explicit ATTACH (DATA_PATH /
        METADATA_SCHEMA) after installing the needed extensions and creating the
        configured secrets — e.g. a Postgres-hosted catalog with Parquet on S3."""
        data_path = cfg.data_path
        if data_path and "://" not in data_path and not Path(data_path).is_absolute():
            resolved_data = self.root / data_path
            resolved_data.mkdir(parents=True, exist_ok=True)
            data_path = str(resolved_data)
        extensions = ["ducklake"]
        if database.removeprefix("ducklake:").startswith("postgres:"):
            extensions.append("postgres")
        if (data_path or "").startswith(("s3://", "gcs://", "r2://")) or any(
            s.type == "s3" for s in cfg.secrets.values()
        ):
            extensions.append("httpfs")
        secrets = [_secret_sql(secret_name, secret) for secret_name, secret in cfg.secrets.items()]
        alias = cfg.alias or (name if name != "default" else (self.config.name or "warehouse"))
        return DuckDBAdapter.connect_ducklake(
            database,
            alias=alias,
            data_path=data_path,
            metadata_schema=cfg.metadata_schema,
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
