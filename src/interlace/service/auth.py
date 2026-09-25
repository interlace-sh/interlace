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
"""

from __future__ import annotations

from litestar.connection import ASGIConnection
from litestar.exceptions import NotAuthorizedException, PermissionDeniedException
from litestar.handlers.base import BaseRouteHandler

from interlace.state.store import event_actor

_OPEN_PATHS = frozenset({"/health", "/"})


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


async def auth_guard(connection: ASGIConnection, route_handler: BaseRouteHandler) -> None:
    path = connection.scope["path"]
    # /ui is the static shell only — every API call it makes still enforces scopes
    if path in _OPEN_PATHS or path.startswith(("/schema", "/ui")):
        return

    store = connection.app.state.store
    if await store.count_api_keys() == 0:
        event_actor.set("anonymous")
        return  # no keys configured -> open (local dev)

    token = _bearer_token(connection, route_handler)
    verified = await store.verify_api_key(token) if token else None
    if verified is None:
        raise NotAuthorizedException(detail="missing or invalid API key")
    name, scopes = verified
    event_actor.set(name)

    required = route_handler.opt.get("scope", "read")
    if required not in scopes and "admin" not in scopes:
        raise PermissionDeniedException(detail=f"requires scope: {required}")
