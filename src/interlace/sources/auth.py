"""Authentication strategies for the REST source client.

Each :class:`Auth` contributes headers and/or query params to every request.
A credential can be passed directly or named as an environment variable, so a
template's model code references the variable and the secret never lands in the
repository. Kept deliberately small — bearer, API key (header or query param),
and basic cover the overwhelming majority of open REST APIs.
"""

from __future__ import annotations

import base64
import os


class Auth:
    """Contribute credentials to a request. Subclasses override one or both."""

    def headers(self) -> dict[str, str]:
        return {}

    def params(self) -> dict[str, str]:
        return {}


class NoAuth(Auth):
    """No credentials — the default, and all a public API needs."""


def _resolve(value: str | None, env: str | None, what: str) -> str:
    """A literal value wins; otherwise read ``env`` from the environment. Raising
    here (rather than sending an empty credential) turns a missing secret into a
    clear message instead of a confusing 401 from the API."""
    if value is not None:
        return value
    if env is not None:
        got = os.environ.get(env)
        if not got:
            raise ValueError(f"{what}: environment variable {env!r} is not set")
        return got
    raise ValueError(f"{what}: pass a token/key value or an env= variable name to read it from")


class BearerAuth(Auth):
    """``Authorization: Bearer <token>`` — GitHub, Stripe, most OAuth2 APIs."""

    def __init__(self, token: str | None = None, *, env: str | None = None) -> None:
        self._token = token
        self._env = env

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {_resolve(self._token, self._env, 'BearerAuth')}"}


class ApiKeyAuth(Auth):
    """An API key in a header (default) or, if ``param`` is set, a query parameter.
    ``scheme`` prefixes the header value (e.g. ``scheme='Token'`` →
    ``Authorization: Token <key>``)."""

    def __init__(
        self,
        key: str | None = None,
        *,
        env: str | None = None,
        header: str | None = "X-API-Key",
        param: str | None = None,
        scheme: str | None = None,
    ) -> None:
        self._key = key
        self._env = env
        self._header = header
        self._param = param
        self._scheme = scheme

    def _value(self) -> str:
        return _resolve(self._key, self._env, "ApiKeyAuth")

    def headers(self) -> dict[str, str]:
        if self._param is not None:
            return {}
        value = self._value()
        return {self._header or "X-API-Key": f"{self._scheme} {value}" if self._scheme else value}

    def params(self) -> dict[str, str]:
        return {self._param: self._value()} if self._param is not None else {}


class BasicAuth(Auth):
    """HTTP Basic — ``Authorization: Basic base64(user:pass)``."""

    def __init__(self, username: str, password: str) -> None:
        self._username = username
        self._password = password

    def headers(self) -> dict[str, str]:
        token = base64.b64encode(f"{self._username}:{self._password}".encode()).decode()
        return {"Authorization": f"Basic {token}"}
