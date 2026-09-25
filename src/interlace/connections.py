"""Named connections for sources that are not warehouse engines.

``interlace.yaml`` ``connections:`` holds ``http`` and ``postgres`` entries.
Python models call :func:`connection` during a build; the apply path binds the
project's connections for that call. Secret values stay in the config object
the model receives — :func:`redacted` is what the API and UI show.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from contextvars import ContextVar, Token
from typing import Any

from interlace.config.config import ConnectionConfig, HttpConnection, PostgresConnection, redact_dsn
from interlace.exceptions import ConfigurationError

_active: ContextVar[Mapping[str, ConnectionConfig] | None] = ContextVar("interlace_connections", default=None)
_SENSITIVE_HEADER = re.compile(r"(?i)password|token|secret|key|authorization|pwd")


def bind_connections(
    connections: Mapping[str, ConnectionConfig] | None,
) -> Token[Mapping[str, ConnectionConfig] | None] | None:
    """Make ``connections`` visible to :func:`connection` on this task. ``None`` binds nothing."""
    if connections is None:
        return None
    return _active.set(dict(connections))


def unbind_connections(token: Token[Mapping[str, ConnectionConfig] | None] | None) -> None:
    if token is not None:
        _active.reset(token)


def connection(name: str) -> ConnectionConfig:
    """The named connection for the build that is running.

    Raises :class:`ConfigurationError` when nothing is bound or the name is unknown.
    """
    current = _active.get()
    if current is None:
        raise ConfigurationError(f"connection {name!r} is not available; it resolves only while a model is building")
    found = current.get(name)
    if found is None:
        known = ", ".join(sorted(current)) or "(none)"
        raise ConfigurationError(f"unknown connection {name!r}; configured: {known}")
    return found


def redacted(name: str, item: ConnectionConfig) -> dict[str, Any]:
    """Name, type, and secret-stripped fields for ``GET /connections`` and the UI."""
    if isinstance(item, HttpConnection):
        headers = {key: ("…" if _SENSITIVE_HEADER.search(key) else value) for key, value in item.headers.items()}
        return {"name": name, "type": "http", "base_url": redact_dsn(item.base_url), "headers": headers}
    if isinstance(item, PostgresConnection):
        return {"name": name, "type": "postgres", "dsn": redact_dsn(item.dsn)}
    raise ConfigurationError(f"connection {name!r} has an unsupported type")
