"""
API key authentication middleware.

Protects API endpoints with configurable API key authentication.
Supports bearer tokens and custom header API keys with per-key
permissions and path whitelisting.
"""

import hashlib
import hmac
import time
from dataclasses import dataclass, field
from typing import Any

from aiohttp import web

from interlace.service.api.errors import APIError, ErrorCode
from interlace.utils.logging import get_logger

logger = get_logger("interlace.service.api.middleware.auth")


@dataclass
class APIKey:
    """A configured API key with associated permissions."""

    name: str
    key: str
    permissions: list[str] = field(default_factory=lambda: ["read"])


@dataclass
class RateLimitState:
    """Token bucket state for a single client."""

    tokens: float
    last_refill: float


class AuthConfig:
    """Parsed authentication configuration."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.enabled: bool = config.get("enabled", False)
        self.api_keys: list[APIKey] = []
        self.whitelist: list[str] = config.get("whitelist", [])
        self.rate_limit: dict[str, Any] = config.get("rate_limit", {})

        for key_conf in config.get("api_keys", []):
            name = key_conf.get("name", "unnamed")
            key_value = key_conf.get("key", "")
            if not key_value:
                logger.warning(f"API key '{name}' has no key value, skipping")
                continue
            perms = key_conf.get("permissions", ["read"])
            self.api_keys.append(APIKey(name=name, key=key_value, permissions=perms))

        # Build a lookup by key hash for constant-time comparison
        self._key_lookup: dict[str, APIKey] = {}
        for api_key in self.api_keys:
            key_hash = hashlib.sha256(api_key.key.encode()).hexdigest()
            self._key_lookup[key_hash] = api_key

    def find_key(self, provided: str) -> APIKey | None:
        """Find an API key by value using constant-time comparison."""
        provided_hash = hashlib.sha256(provided.encode()).hexdigest()
        for stored_hash, api_key in self._key_lookup.items():
            if hmac.compare_digest(provided_hash, stored_hash):
                return api_key
        return None


# HTTP methods → required permission
_METHOD_PERMISSIONS: dict[str, str] = {
    "GET": "read",
    "HEAD": "read",
    "OPTIONS": "read",
    "POST": "write",
    "PUT": "write",
    "PATCH": "write",
    "DELETE": "write",
}

# Override: run-triggering endpoints require "execute"
_EXECUTE_PATHS: set[str] = {
    "/api/v1/runs",
    "/api/v1/lineage/refresh",
}


def _required_permission(method: str, path: str) -> str:
    """Determine the permission required for a given request."""
    if method == "POST" and any(path.startswith(p) for p in _EXECUTE_PATHS):
        return "execute"
    return _METHOD_PERMISSIONS.get(method, "read")


def _is_whitelisted(path: str, whitelist: list[str]) -> bool:
    """Check if a path matches any whitelist entry.

    Supports exact matches and prefix matches (entries ending with *)."""
    for entry in whitelist:
        if entry.endswith("*"):
            if path.startswith(entry[:-1]):
                return True
        elif path == entry:
            return True
    return False


def setup_auth(app: web.Application, config: dict[str, Any]) -> None:
    """
    Setup API key authentication middleware.

    Config schema (from config.yaml):
        service:
          auth:
            enabled: true
            api_keys:
              - name: "ci-pipeline"
                key: "${INTERLACE_API_KEY}"
                permissions: ["read", "write", "execute"]
            whitelist:
              - "/health"
              - "/api/v1/health"
              - "/api/v1/events"
            rate_limit:
              requests_per_second: 100
              burst: 200

    Args:
        app: aiohttp Application
        config: Auth configuration dict from ``config["service"]["auth"]``
    """
    auth_config = AuthConfig(config)
    app[web.AppKey("auth_config", AuthConfig)] = auth_config

    if not auth_config.enabled:
        logger.debug("API authentication disabled")
        return

    if not auth_config.api_keys:
        logger.warning("Auth enabled but no API keys configured — all requests will be rejected")

    # Rate limit state: keyed by API key name (or "anonymous")
    rate_state: dict[str, RateLimitState] = {}
    rate_rps = float(auth_config.rate_limit.get("requests_per_second", 0))
    rate_burst = float(auth_config.rate_limit.get("burst", rate_rps * 2 if rate_rps > 0 else 0))

    @web.middleware
    async def auth_middleware(request: web.Request, handler: Any) -> web.Response:
        path = request.path

        # Always allow whitelisted paths
        if _is_whitelisted(path, auth_config.whitelist):
            return await handler(request)  # type: ignore[no-any-return]

        # Skip auth for non-API paths (static files, UI, etc.)
        if not path.startswith("/api/"):
            return await handler(request)  # type: ignore[no-any-return]

        # Extract token from Authorization header or X-API-Key header
        token: str | None = None
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        if not token:
            token = request.headers.get("X-API-Key") or None

        if not token:
            raise APIError(
                code=ErrorCode.UNAUTHORIZED,
                message="Missing API key. Provide via Authorization: Bearer <key> or X-API-Key header.",
                status=401,
            )

        api_key = auth_config.find_key(token)
        if api_key is None:
            raise APIError(
                code=ErrorCode.UNAUTHORIZED,
                message="Invalid API key",
                status=401,
            )

        # Check permissions
        required = _required_permission(request.method, path)
        if required not in api_key.permissions:
            raise APIError(
                code=ErrorCode.FORBIDDEN,
                message=f"API key '{api_key.name}' lacks '{required}' permission",
                status=403,
                details={"required_permission": required, "key_name": api_key.name},
            )

        # Rate limiting (token bucket)
        if rate_rps > 0:
            now = time.monotonic()
            state = rate_state.get(api_key.name)
            if state is None:
                state = RateLimitState(tokens=rate_burst, last_refill=now)
                rate_state[api_key.name] = state

            # Refill tokens
            elapsed = now - state.last_refill
            state.tokens = min(rate_burst, state.tokens + elapsed * rate_rps)
            state.last_refill = now

            if state.tokens < 1.0:
                retry_after = (1.0 - state.tokens) / rate_rps
                raise APIError(
                    code=ErrorCode.RATE_LIMITED,
                    message=f"Rate limit exceeded for key '{api_key.name}'",
                    status=429,
                    details={
                        "limit": rate_rps,
                        "retry_after": round(retry_after, 2),
                    },
                )
            state.tokens -= 1.0

        # Attach key info to request for downstream handlers
        request["auth_key"] = api_key

        return await handler(request)  # type: ignore[no-any-return]

    # Insert after CORS (position 1) but before error handler
    # Middleware chain: CORS → auth → error → handler
    app.middlewares.append(auth_middleware)
    logger.info(f"API authentication enabled with {len(auth_config.api_keys)} key(s)")
