"""The ``@model`` / ``@stream`` / ``@check`` decorator DX and the in-process registry.

These decorators only *declare* intent — they capture metadata into a registry and
return the original function unchanged, so models stay ordinary, testable Python.
Compilation, planning, and execution happen later over the registry. UK spelling
``materialise`` is intentional (matches v0.x and the config schema).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from interlace.exceptions import DefinitionError
from interlace.exports import ExportConfig

ModelFn = Callable[..., Any]

_MATERIALISATIONS = frozenset({"table", "view", "ephemeral", "incremental", "none"})
_KINDS = frozenset({"batch", "incremental", "incremental_stream"})
_DRIFT_MODES = frozenset({"evolve", "reject", "quarantine"})


def _as_tuple(value: str | Sequence[str]) -> tuple[str, ...]:
    return (value,) if isinstance(value, str) else tuple(value)


def _as_columns(value: dict[str, str | None] | Sequence[str] | None) -> dict[str, str | None] | None:
    """Normalise a column contract: a list of names -> {name: None}; a mapping kept as name->type."""
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(name): (str(dtype) if dtype is not None else None) for name, dtype in value.items()}
    return {str(name): None for name in value}


def _as_export(value: ExportConfig | dict[str, Any] | None) -> ExportConfig | None:
    if value is None or isinstance(value, ExportConfig):
        return value
    return ExportConfig.from_dict(value)


@dataclass
class ModelDef:
    """Declared metadata for one model (Python or SQL)."""

    name: str
    fn: ModelFn | None = None
    sql: str | None = None
    materialise: str = "table"
    strategy: str = "full"
    key: tuple[str, ...] = ()
    dialect: str | None = None
    depends_on: tuple[str, ...] = ()
    kind: str = "batch"
    interval: str | None = None  # grain for incremental_by_time (e.g. "1d")
    time_column: str | None = None  # partition column for incremental_by_time
    tags: tuple[str, ...] = ()
    owner: str | None = None
    description: str | None = None
    columns: dict[str, str | None] | None = None  # output contract: column -> type (None = any)
    export: ExportConfig | None = None  # presence makes this model a sink (no table/view)


@dataclass
class StreamDef:
    """Declared metadata for one durable ingestion stream."""

    name: str
    schema: dict[str, str]
    idempotency_key: str | None = None
    retention: str | None = None
    on_schema_drift: str = "reject"
    rate_limit: str | None = None


@dataclass
class CheckDef:
    """Declared metadata for one data check; results gate promotion."""

    name: str
    model: str
    fn: ModelFn
    severity: str = "error"


@dataclass
class Registry:
    """Holds everything declared in a project. One instance per process."""

    models: dict[str, ModelDef] = field(default_factory=dict)
    streams: dict[str, StreamDef] = field(default_factory=dict)
    checks: list[CheckDef] = field(default_factory=list)

    def register_model(self, definition: ModelDef) -> None:
        if definition.name in self.models:
            raise DefinitionError(f"duplicate model name: {definition.name!r}")
        self.models[definition.name] = definition

    def register_stream(self, definition: StreamDef) -> None:
        if definition.name in self.streams:
            raise DefinitionError(f"duplicate stream name: {definition.name!r}")
        self.streams[definition.name] = definition

    def register_check(self, definition: CheckDef) -> None:
        self.checks.append(definition)

    def clear(self) -> None:
        self.models.clear()
        self.streams.clear()
        self.checks.clear()


REGISTRY = Registry()


def model(
    name: str | None = None,
    *,
    materialise: str = "table",
    strategy: str = "full",
    key: str | Sequence[str] = (),
    dialect: str | None = None,
    depends_on: str | Sequence[str] = (),
    kind: str = "batch",
    interval: str | None = None,
    time_column: str | None = None,
    tags: str | Sequence[str] = (),
    owner: str | None = None,
    description: str | None = None,
    columns: dict[str, str | None] | Sequence[str] | None = None,
    export: ExportConfig | dict[str, Any] | None = None,
) -> Callable[[ModelFn], ModelFn]:
    """Declare a Python model. The function returns a ``Relation`` (or composes one)."""
    if materialise not in _MATERIALISATIONS:
        raise DefinitionError(f"unknown materialise {materialise!r}; expected one of {sorted(_MATERIALISATIONS)}")
    if materialise == "ephemeral":
        raise DefinitionError("Python models cannot be ephemeral; ephemeral requires SQL (it is inlined as a CTE)")
    if kind not in _KINDS:
        raise DefinitionError(f"unknown kind {kind!r}; expected one of {sorted(_KINDS)}")

    def decorator(fn: ModelFn) -> ModelFn:
        REGISTRY.register_model(
            ModelDef(
                name=name or fn.__name__,
                fn=fn,
                materialise=materialise,
                strategy=strategy,
                key=_as_tuple(key),
                dialect=dialect,
                depends_on=_as_tuple(depends_on),
                kind=kind,
                interval=interval,
                time_column=time_column,
                tags=_as_tuple(tags),
                owner=owner,
                description=description,
                columns=_as_columns(columns),
                export=_as_export(export),
            )
        )
        return fn

    return decorator


def stream(
    name: str,
    *,
    schema: dict[str, str],
    idempotency_key: str | None = None,
    retention: str | None = None,
    on_schema_drift: str = "reject",
    rate_limit: str | None = None,
) -> Callable[[ModelFn], ModelFn]:
    """Declare a durable ingestion stream with an HTTP publish endpoint."""
    if on_schema_drift not in _DRIFT_MODES:
        raise DefinitionError(f"unknown on_schema_drift {on_schema_drift!r}; expected one of {sorted(_DRIFT_MODES)}")

    def decorator(fn: ModelFn) -> ModelFn:
        REGISTRY.register_stream(
            StreamDef(
                name=name,
                schema=schema,
                idempotency_key=idempotency_key,
                retention=retention,
                on_schema_drift=on_schema_drift,
                rate_limit=rate_limit,
            )
        )
        return fn

    return decorator


def check(*, model: str, name: str | None = None, severity: str = "error") -> Callable[[ModelFn], ModelFn]:
    """Declare a data check bound to a model. Failure at error severity blocks promotion."""

    def decorator(fn: ModelFn) -> ModelFn:
        REGISTRY.register_check(CheckDef(name=name or fn.__name__, model=model, fn=fn, severity=severity))
        return fn

    return decorator
