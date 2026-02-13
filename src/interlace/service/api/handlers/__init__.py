"""
API endpoint handlers.

Each handler class manages a resource type (models, flows, etc.).
"""

from typing import TYPE_CHECKING, Any

from aiohttp import web

if TYPE_CHECKING:
    from interlace.service.server import InterlaceService


class BaseHandler:
    """
    Base class for API handlers.

    Provides access to service components and common utilities.
    """

    def __init__(self, service: "InterlaceService"):
        self.service = service

    @property
    def config(self):
        """Get service configuration."""
        return self.service.config

    @property
    def models(self) -> dict[str, dict[str, Any]]:
        """Get discovered models."""
        return self.service.models

    @property
    def graph(self):
        """Get dependency graph."""
        return self.service.graph

    @property
    def state_store(self):
        """Get state store for historical data."""
        return self.service.state_store

    @property
    def event_bus(self):
        """Get event bus for real-time updates."""
        return self.service.event_bus

    def get_request_id(self, request: web.Request) -> str | None:
        """Get request ID from request context."""
        return request.get("request_id")

    async def json_response(
        self,
        data: Any,
        status: int = 200,
        request: web.Request | None = None,
    ) -> web.Response:
        """Create JSON response with standard headers."""
        headers = {}
        if request:
            request_id = self.get_request_id(request)
            if request_id:
                headers["X-Request-ID"] = request_id
        return web.json_response(data, status=status, headers=headers)
