"""DuckDB external inputs: a config entry becomes a view models can ``FROM``.

``parquet`` / ``csv`` / ``json`` / ``delta`` / ``iceberg`` compile to the matching
DuckDB scan. ``httpfs``, ``delta``, or ``iceberg`` is loaded when the path or
format needs it. Other engines do not grow a scanner — a model on one of those
engines that references an input fails at compile.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from sqlglot import exp

from interlace.config.config import ConnectionConfig, HttpConnection, InputConfig
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import CompilationError, ConfigurationError
from interlace.graph.project import CompiledProject
from interlace.ir.canonicalize import table_references
from interlace.sinks import expand_path_tokens

_SCANS = {
    "parquet": "read_parquet",
    "csv": "read_csv",
    "json": "read_json",
    "delta": "delta_scan",
    "iceberg": "iceberg_scan",
}
_FORMAT_EXTENSIONS = {"delta": "delta", "iceberg": "iceberg"}
_REMOTE = ("s3://", "gs://", "gcs://", "r2://", "http://", "https://")
_DUCKDB_TYPES = frozenset({"duckdb", "ducklake"})
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def install_inputs(
    engine: DuckDBAdapter,
    inputs: dict[str, InputConfig],
    *,
    workspace: str,
    connections: Mapping[str, ConnectionConfig],
    secret_sql: dict[str, str],
) -> None:
    """Create or replace one view per input on an open DuckDB engine."""
    for name, spec in inputs.items():
        if not _NAME.fullmatch(name):
            raise ConfigurationError(f"input {name!r} must be a bare SQL name")
        path = expand_path_tokens(spec.path, workspace=workspace)
        for extension in _extensions(spec, path):
            _load_extension(engine, extension, input_name=name)
        if spec.connection:
            _install_connection(engine, spec.connection, connections, secret_sql)
        scan = _SCANS[spec.format]
        escaped = path.replace("'", "''")
        options = ", header=true" if spec.format == "csv" else ""
        view = exp.to_identifier(name).sql(dialect="duckdb")
        engine.execute_sync(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM {scan}('{escaped}'{options})")


def reject_inputs_on_other_engines(
    compiled: CompiledProject,
    inputs: dict[str, InputConfig],
    engine_types: dict[str, str],
) -> None:
    """A non-DuckDB model that reads an input cannot be scanned on that engine."""
    if not inputs:
        return
    overlap = set(inputs) & set(compiled.models)
    if overlap:
        raise ConfigurationError(f"inputs share names with models: {', '.join(sorted(overlap))}")
    for model in compiled.models.values():
        if model.ast is None:
            continue
        used = _referenced_inputs(model.ast, set(inputs))
        if not used:
            continue
        engine_type = engine_types.get(model.engine, "duckdb")
        if engine_type not in _DUCKDB_TYPES:
            raise CompilationError(
                f"model {model.name!r} reads input {used[0]!r} on engine {model.engine!r} "
                f"({engine_type}), and inputs are DuckDB scans"
            )


def _referenced_inputs(ast: object, names: set[str]) -> list[str]:
    from sqlglot import exp as expression

    if not isinstance(ast, expression.Expression):
        return []
    found: list[str] = []
    for ref in table_references(ast):
        bare = ref.rsplit(".", 1)[-1]
        if ref in names or bare in names:
            found.append(ref if ref in names else bare)
    return found


def _extensions(spec: InputConfig, path: str) -> list[str]:
    found: list[str] = []
    if spec.format in _FORMAT_EXTENSIONS:
        found.append(_FORMAT_EXTENSIONS[spec.format])
    if path.startswith(_REMOTE):
        found.append("httpfs")
    return found


def _load_extension(engine: DuckDBAdapter, name: str, *, input_name: str) -> None:
    try:
        engine.execute_sync(f"LOAD {name}")
    except Exception:
        try:
            engine.execute_sync(f"INSTALL {name}")
            engine.execute_sync(f"LOAD {name}")
        except Exception as exc:
            raise ConfigurationError(
                f"input {input_name!r} needs the DuckDB {name!r} extension, and it is not available ({exc})"
            ) from exc


def _install_connection(
    engine: DuckDBAdapter,
    name: str,
    connections: Mapping[str, ConnectionConfig],
    secret_sql: dict[str, str],
) -> None:
    if name in secret_sql:
        engine.execute_sync(secret_sql[name])
        return
    item = connections.get(name)
    if isinstance(item, HttpConnection):
        pairs = ", ".join(
            f"'{key.replace(chr(39), chr(39) * 2)}': '{value.replace(chr(39), chr(39) * 2)}'"
            for key, value in item.headers.items()
        )
        ident = exp.to_identifier(name).sql(dialect="duckdb")
        engine.execute_sync(f"CREATE OR REPLACE SECRET {ident} (TYPE http, EXTRA_HTTP_HEADERS MAP {{{pairs}}})")
        return
    if item is not None:
        raise ConfigurationError(
            f"input connection {name!r} is {getattr(item, 'type', 'unknown')}; "
            "file inputs need an http connection or a secret"
        )
    known = ", ".join(sorted({*connections, *secret_sql})) or "(none)"
    raise ConfigurationError(f"unknown input connection {name!r}; configured: {known}")
