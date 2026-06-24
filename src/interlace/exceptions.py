"""Exception hierarchy. Everything inherits from :class:`InterlaceError`.

Ported in spirit from v0.x: a single root with an optional structured ``details``
payload so errors carry machine-readable context to the CLI, API, and event log.
"""

from __future__ import annotations

from typing import Any


class InterlaceError(Exception):
    """Base class for all Interlace errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ConfigurationError(InterlaceError):
    """Invalid or missing project/model configuration."""


class DefinitionError(InterlaceError):
    """A model, stream, or check is declared incorrectly."""


class CompilationError(InterlaceError):
    """SQL could not be parsed, qualified, or transpiled."""


class DependencyError(InterlaceError):
    """The dependency graph is invalid (e.g. a cycle)."""


class SelectionError(InterlaceError):
    """A model selector could not be resolved."""


class EngineError(InterlaceError):
    """An engine adapter failed to execute, fetch, or load."""


class StateError(InterlaceError):
    """The state store could not be read or written consistently."""


class PlanError(InterlaceError):
    """A plan could not be computed or applied."""


class SchemaError(InterlaceError):
    """A model's built schema violates its declared column contract."""


class StreamError(InterlaceError):
    """Stream ingestion, the durable log, or a consumer failed."""


class Backpressure(StreamError):
    """The durable log's bounded commit queue is full; callers should retry (HTTP 429)."""


class CheckError(InterlaceError):
    """A data check failed with error severity."""
