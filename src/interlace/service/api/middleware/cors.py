"""
CORS middleware configuration.

Enables Cross-Origin Resource Sharing for the API.
"""

from typing import Any

from aiohttp import web


def setup_cors(
    app: web.Application,
    origins: list[str] | None = None,
    allow_credentials: bool = True,
    allow_methods: list[str] | None = None,
    allow_headers: list[str] | None = None,
    expose_headers: list[str] | None = None,
    max_age: int = 3600,
) -> None:
    """
    Setup CORS middleware for the application.

    Args:
        app: aiohttp Application
        origins: Allowed origins (default: ["*"])
        allow_credentials: Allow credentials (default: True)
        allow_methods: Allowed HTTP methods
        allow_headers: Allowed request headers
        expose_headers: Headers to expose to browser
        max_age: Preflight cache duration in seconds
    """
    if origins is None:
        origins = ["*"]
    if allow_methods is None:
        allow_methods = ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]
    if allow_headers is None:
        allow_headers = ["*"]
    if expose_headers is None:
        expose_headers = ["X-Request-ID", "X-RateLimit-Remaining"]

    # Store CORS config in app
    app["cors_config"] = {
        "origins": origins,
        "allow_credentials": allow_credentials,
        "allow_methods": allow_methods,
        "allow_headers": allow_headers,
        "expose_headers": expose_headers,
        "max_age": max_age,
    }

    # Add CORS middleware
    @web.middleware
    async def cors_middleware(request: web.Request, handler: Any) -> web.Response:
        # Get origin from request
        origin = request.headers.get("Origin", "")

        # Check if origin is allowed
        allowed_origin = None
        if "*" in origins:
            allowed_origin = origin or "*"
        elif origin in origins:
            allowed_origin = origin

        # Handle preflight OPTIONS request
        if request.method == "OPTIONS":
            response = web.Response(status=204)
        else:
            try:
                response = await handler(request)
            except web.HTTPException as e:
                response = e

        # Add CORS headers
        if allowed_origin:
            response.headers["Access-Control-Allow-Origin"] = allowed_origin
            if allow_credentials:
                response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Access-Control-Allow-Methods"] = ", ".join(allow_methods)
            response.headers["Access-Control-Allow-Headers"] = ", ".join(allow_headers)
            response.headers["Access-Control-Expose-Headers"] = ", ".join(expose_headers)
            response.headers["Access-Control-Max-Age"] = str(max_age)

        return response

    # Insert at beginning of middleware chain
    app.middlewares.insert(0, cors_middleware)
