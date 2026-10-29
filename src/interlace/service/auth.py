"""API-key authentication guard.

Keys are ``ilk_…`` bearer tokens, sha256-hashed in the state DB with a scope
list. The guard enforces auth only once at least one key exists — a fresh
project is open for local development, and creating a key (``interlace apikey
create``) locks it down. ``/health`` and the OpenAPI docs (``/schema``) are
always open. Routes declare a required scope via ``opt={"scope": "write"}``
(default ``read``); an ``admin`` scope satisfies any requirement.

Routes with ``opt={"query_token": True}`` (the SSE tails) also accept
``?token=`` because EventSource cannot send an ``Authorization`` header.
Prefer the header everywhere else (tokens in URLs can land in access logs).

An open daemon (no keys yet) used to accept a ``text/plain`` POST from any
origin. Browsers send that without a preflight, so a page open on the same
machine could call ``/reset``. Writes now require ``application/json``, and a
loopback ``serve`` rejects a ``Host`` or ``Origin`` that is not loopback — that
is the DNS-rebinding case, where the browser's host is the attacker's name.
``--allow-open`` on a public bind cannot trust the Host header (the caller
sets it), so it requires the session token printed at startup instead.
"""

from __future__ import annotations

import secrets
from urllib.parse import urlparse

from litestar.connection import ASGIConnection
from litestar.exceptions import NotAuthorizedException, PermissionDeniedException
from litestar.handlers.base import BaseRouteHandler

from interlace.state.store import event_actor

_OPEN_PATHS = frozenset({"/health", "/"})
_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def _bearer_token(connection: ASGIConnection, route_handler: BaseRouteHandler) -> str | None:
    header = connection.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        return header[7:]
    # EventSource cannot set Authorization. The route opts in; other paths do not.
    if route_handler.opt.get("query_token"):
        raw = connection.query_params.get("token")
        if isinstance(raw, str) and raw:
            return raw
    return None


def _hostname(value: str) -> str:
    """Host from a ``Host`` header or an ``Origin`` URL. Empty when it cannot be read."""
    text = value.strip()
    if not text or text == "null":
        return ""
    if "://" in text:
        return (urlparse(text).hostname or "").lower()
    if text.startswith("["):
        end = text.find("]")
        return text[1:end].lower() if end != -1 else ""
    return text.split(":", 1)[0].lower()


def _presented_session(connection: ASGIConnection) -> str:
    header = connection.headers.get("x-interlace-token", "")
    if isinstance(header, str) and header:
        return header
    bearer = connection.headers.get("Authorization", "")
    if isinstance(bearer, str) and bearer.startswith("Bearer "):
        return bearer[7:]
    return ""


def _matches(presented: str, expected: str) -> bool:
    if not presented or len(presented) != len(expected):
        return False
    return secrets.compare_digest(presented, expected)


def _reject_open_write(connection: ASGIConnection) -> None:
    """Close the cross-site write hole while the API still has no keys."""
    method = str(connection.scope.get("method", "GET")).upper()
    if method not in _WRITE_METHODS:
        return
    media = str(connection.headers.get("content-type", "")).split(";", 1)[0].strip().lower()
    length = str(connection.headers.get("content-length", ""))
    if media != "application/json" and not (media == "" and length in {"", "0"}):
        raise PermissionDeniedException(detail="write requests must use Content-Type: application/json")

    allowed = getattr(connection.app.state, "allowed_hosts", None)
    origin = _hostname(str(connection.headers.get("origin", "")))
    if origin:
        if isinstance(allowed, frozenset):
            if origin not in allowed:
                raise PermissionDeniedException(detail="Origin is not allowed for this server")
        elif origin != _hostname(str(connection.headers.get("host", ""))):
            raise PermissionDeniedException(detail="Origin does not match Host")

    token = getattr(connection.app.state, "local_token", None)
    if isinstance(token, str) and token and not _matches(_presented_session(connection), token):
        raise NotAuthorizedException(detail="this server requires the session token printed at startup")


def _reject_foreign_host(connection: ASGIConnection) -> None:
    allowed = getattr(connection.app.state, "allowed_hosts", None)
    if not isinstance(allowed, frozenset):
        return
    host = _hostname(str(connection.headers.get("host", "")))
    if host not in allowed:
        raise PermissionDeniedException(detail="Host is not allowed for this server")


async def auth_guard(connection: ASGIConnection, route_handler: BaseRouteHandler) -> None:
    # A loopback serve sets this. It applies to reads too: a DNS-rebound page
    # sends the attacker's name as Host and would otherwise receive responses.
    _reject_foreign_host(connection)

    path = connection.scope["path"]
    # /ui is the static shell only — every API call it makes still enforces scopes
    if path in _OPEN_PATHS or path.startswith(("/schema", "/ui")):
        return

    store = connection.app.state.store
    if await store.count_api_keys() == 0:
        _reject_open_write(connection)
        event_actor.set("anonymous")
        return  # no keys configured -> open (local dev), writes constrained above

    token = _bearer_token(connection, route_handler)
    verified = await store.verify_api_key(token) if token else None
    if verified is None:
        raise NotAuthorizedException(detail="missing or invalid API key")
    name, scopes = verified
    event_actor.set(name)

    required = route_handler.opt.get("scope", "read")
    if required not in scopes and "admin" not in scopes:
        raise PermissionDeniedException(detail=f"requires scope: {required}")
