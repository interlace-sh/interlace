"""Tests for API key authentication middleware."""

from __future__ import annotations

from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from interlace.service.api.middleware.auth import (
    AuthConfig,
    _is_whitelisted,
    _required_permission,
    setup_auth,
)
from interlace.service.api.middleware.error import error_middleware


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

VALID_KEY = "test-key-12345"
READONLY_KEY = "readonly-key-67890"
EXECUTE_KEY = "execute-key-abcde"


def _make_auth_config(
    enabled: bool = True,
    rate_limit: dict[str, Any] | None = None,
    whitelist: list[str] | None = None,
) -> dict[str, Any]:
    keys = [
        {"name": "full-access", "key": VALID_KEY, "permissions": ["read", "write", "execute"]},
        {"name": "read-only", "key": READONLY_KEY, "permissions": ["read"]},
        {"name": "executor", "key": EXECUTE_KEY, "permissions": ["read", "execute"]},
    ]
    conf: dict[str, Any] = {"enabled": enabled, "api_keys": keys}
    if whitelist is not None:
        conf["whitelist"] = whitelist
    if rate_limit is not None:
        conf["rate_limit"] = rate_limit
    return conf


async def _dummy_handler(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


def _build_app(
    auth_config: dict[str, Any] | None = None,
    routes: list[web.RouteDef] | None = None,
) -> web.Application:
    """Build a minimal app with auth + error middleware and test routes."""
    app = web.Application(middlewares=[error_middleware])
    if auth_config is not None:
        setup_auth(app, auth_config)
    if routes is None:
        routes = [
            web.get("/api/v1/models", _dummy_handler),
            web.post("/api/v1/runs", _dummy_handler),
            web.get("/api/v1/health", _dummy_handler),
            web.get("/health", _dummy_handler),
            web.get("/not-api", _dummy_handler),
        ]
    app.router.add_routes(routes)
    return app


async def _make_client(app: web.Application) -> TestClient:
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    return client


# ---------------------------------------------------------------------------
# Unit tests for helper functions
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAuthHelpers:
    def test_required_permission_get(self) -> None:
        assert _required_permission("GET", "/api/v1/models") == "read"

    def test_required_permission_post(self) -> None:
        assert _required_permission("POST", "/api/v1/streams/foo") == "write"

    def test_required_permission_execute(self) -> None:
        assert _required_permission("POST", "/api/v1/runs") == "execute"

    def test_required_permission_lineage_refresh(self) -> None:
        assert _required_permission("POST", "/api/v1/lineage/refresh") == "execute"

    def test_whitelist_exact(self) -> None:
        assert _is_whitelisted("/health", ["/health"])
        assert not _is_whitelisted("/health2", ["/health"])

    def test_whitelist_prefix(self) -> None:
        assert _is_whitelisted("/api/v1/health", ["/api/v1/health*"])
        assert _is_whitelisted("/api/v1/health/deep", ["/api/v1/health*"])
        assert not _is_whitelisted("/api/v1/models", ["/api/v1/health*"])

    def test_whitelist_empty(self) -> None:
        assert not _is_whitelisted("/anything", [])


@pytest.mark.unit
class TestAuthConfig:
    def test_parse_keys(self) -> None:
        conf = AuthConfig(_make_auth_config())
        assert len(conf.api_keys) == 3
        assert conf.enabled is True

    def test_find_key_valid(self) -> None:
        conf = AuthConfig(_make_auth_config())
        key = conf.find_key(VALID_KEY)
        assert key is not None
        assert key.name == "full-access"

    def test_find_key_invalid(self) -> None:
        conf = AuthConfig(_make_auth_config())
        assert conf.find_key("bad-key") is None

    def test_disabled(self) -> None:
        conf = AuthConfig(_make_auth_config(enabled=False))
        assert conf.enabled is False

    def test_empty_key_skipped(self) -> None:
        config: dict[str, Any] = {"enabled": True, "api_keys": [{"name": "empty", "key": ""}]}
        conf = AuthConfig(config)
        assert len(conf.api_keys) == 0


# ---------------------------------------------------------------------------
# Integration tests with aiohttp test client
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAuthMiddlewareEnabled:
    async def test_missing_key_returns_401(self) -> None:
        app = _build_app(_make_auth_config(whitelist=["/api/v1/health"]))
        client = await _make_client(app)
        try:
            resp = await client.get("/api/v1/models")
            assert resp.status == 401
            body = await resp.json()
            assert body["error"]["code"] == "UNAUTHORIZED"
        finally:
            await client.close()

    async def test_invalid_key_returns_401(self) -> None:
        app = _build_app(_make_auth_config(whitelist=["/api/v1/health"]))
        client = await _make_client(app)
        try:
            resp = await client.get("/api/v1/models", headers={"Authorization": "Bearer wrong-key"})
            assert resp.status == 401
        finally:
            await client.close()

    async def test_valid_bearer_returns_200(self) -> None:
        app = _build_app(_make_auth_config(whitelist=["/api/v1/health"]))
        client = await _make_client(app)
        try:
            resp = await client.get("/api/v1/models", headers={"Authorization": f"Bearer {VALID_KEY}"})
            assert resp.status == 200
        finally:
            await client.close()

    async def test_valid_api_key_header_returns_200(self) -> None:
        app = _build_app(_make_auth_config(whitelist=["/api/v1/health"]))
        client = await _make_client(app)
        try:
            resp = await client.get("/api/v1/models", headers={"X-API-Key": VALID_KEY})
            assert resp.status == 200
        finally:
            await client.close()

    async def test_whitelisted_path_no_auth(self) -> None:
        app = _build_app(_make_auth_config(whitelist=["/api/v1/health"]))
        client = await _make_client(app)
        try:
            resp = await client.get("/api/v1/health")
            assert resp.status == 200
        finally:
            await client.close()

    async def test_non_api_path_no_auth(self) -> None:
        app = _build_app(_make_auth_config(whitelist=["/api/v1/health"]))
        client = await _make_client(app)
        try:
            resp = await client.get("/not-api")
            assert resp.status == 200
        finally:
            await client.close()

    async def test_root_health_no_auth(self) -> None:
        app = _build_app(_make_auth_config(whitelist=["/api/v1/health"]))
        client = await _make_client(app)
        try:
            resp = await client.get("/health")
            assert resp.status == 200
        finally:
            await client.close()


@pytest.mark.unit
class TestAuthPermissions:
    async def test_readonly_can_get(self) -> None:
        app = _build_app(_make_auth_config())
        client = await _make_client(app)
        try:
            resp = await client.get("/api/v1/models", headers={"Authorization": f"Bearer {READONLY_KEY}"})
            assert resp.status == 200
        finally:
            await client.close()

    async def test_readonly_cannot_post(self) -> None:
        app = _build_app(_make_auth_config())
        client = await _make_client(app)
        try:
            resp = await client.post("/api/v1/runs", headers={"Authorization": f"Bearer {READONLY_KEY}"})
            assert resp.status == 403
            body = await resp.json()
            assert body["error"]["code"] == "FORBIDDEN"
        finally:
            await client.close()

    async def test_executor_can_post_runs(self) -> None:
        app = _build_app(_make_auth_config())
        client = await _make_client(app)
        try:
            resp = await client.post("/api/v1/runs", headers={"Authorization": f"Bearer {EXECUTE_KEY}"})
            assert resp.status == 200
        finally:
            await client.close()

    async def test_executor_cannot_post_write(self) -> None:
        app = _build_app(
            _make_auth_config(),
            routes=[web.post("/api/v1/streams/test", _dummy_handler)],
        )
        client = await _make_client(app)
        try:
            resp = await client.post(
                "/api/v1/streams/test",
                headers={"Authorization": f"Bearer {EXECUTE_KEY}"},
            )
            assert resp.status == 403
        finally:
            await client.close()

    async def test_full_access_can_do_anything(self) -> None:
        app = _build_app(_make_auth_config())
        client = await _make_client(app)
        try:
            resp = await client.get("/api/v1/models", headers={"Authorization": f"Bearer {VALID_KEY}"})
            assert resp.status == 200
            resp = await client.post("/api/v1/runs", headers={"Authorization": f"Bearer {VALID_KEY}"})
            assert resp.status == 200
        finally:
            await client.close()


@pytest.mark.unit
class TestAuthDisabled:
    async def test_no_auth_required_when_disabled(self) -> None:
        app = _build_app(_make_auth_config(enabled=False))
        client = await _make_client(app)
        try:
            resp = await client.get("/api/v1/models")
            assert resp.status == 200
        finally:
            await client.close()

    async def test_no_config_no_auth(self) -> None:
        app = _build_app(auth_config=None)
        client = await _make_client(app)
        try:
            resp = await client.get("/api/v1/models")
            assert resp.status == 200
        finally:
            await client.close()


@pytest.mark.unit
class TestRateLimiting:
    async def test_rate_limit_allows_burst(self) -> None:
        config = _make_auth_config(rate_limit={"requests_per_second": 5, "burst": 5})
        app = _build_app(config)
        client = await _make_client(app)
        try:
            headers = {"Authorization": f"Bearer {VALID_KEY}"}
            for _ in range(5):
                resp = await client.get("/api/v1/models", headers=headers)
                assert resp.status == 200
        finally:
            await client.close()

    async def test_rate_limit_exceeded(self) -> None:
        config = _make_auth_config(rate_limit={"requests_per_second": 5, "burst": 5})
        app = _build_app(config)
        client = await _make_client(app)
        try:
            headers = {"Authorization": f"Bearer {VALID_KEY}"}
            # Exhaust burst
            for _ in range(5):
                await client.get("/api/v1/models", headers=headers)

            # Next request should be rate limited
            resp = await client.get("/api/v1/models", headers=headers)
            assert resp.status == 429
            body = await resp.json()
            assert body["error"]["code"] == "RATE_LIMITED"
        finally:
            await client.close()

    async def test_rate_limit_per_key(self) -> None:
        config = _make_auth_config(rate_limit={"requests_per_second": 5, "burst": 5})
        app = _build_app(config)
        client = await _make_client(app)
        try:
            # Exhaust full-access key's burst
            for _ in range(5):
                await client.get("/api/v1/models", headers={"Authorization": f"Bearer {VALID_KEY}"})

            # read-only key should still have its own bucket
            resp = await client.get("/api/v1/models", headers={"Authorization": f"Bearer {READONLY_KEY}"})
            assert resp.status == 200
        finally:
            await client.close()

    async def test_no_rate_limit_when_zero(self) -> None:
        config = _make_auth_config(rate_limit={"requests_per_second": 0})
        app = _build_app(config)
        client = await _make_client(app)
        try:
            headers = {"Authorization": f"Bearer {VALID_KEY}"}
            for _ in range(20):
                resp = await client.get("/api/v1/models", headers=headers)
                assert resp.status == 200
        finally:
            await client.close()
