"""
API endpoint handlers.

Each handler class manages a resource type (models, flows, etc.).
"""

import datetime
import json
import math
from typing import TYPE_CHECKING, Any

from aiohttp import web

if TYPE_CHECKING:
    from interlace.service.server import InterlaceService


def _sanitize(obj: Any) -> Any:
    """Recursively sanitize data for JSON serialization.

    Converts NaN/Inf floats to None, pandas Timestamps to ISO strings,
    numpy types to native Python, and pandas NA/NaT to None.
    """
    if obj is None:
        return None

    # Check for float NaN/Inf first (most common issue from DuckDB)
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    if isinstance(obj, (str, int, bool)):
        return obj

    # Handle pandas NA/NaT
    try:
        import pandas as pd

        if isinstance(obj, type(pd.NaT)) or pd.isna(obj):
            return None
    except (ImportError, TypeError, ValueError):
        pass

    # Handle datetime types (including pandas Timestamp)
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()

    # Handle numpy arrays and scalars
    if hasattr(obj, "tolist") and hasattr(obj, "dtype") and hasattr(obj, "shape"):
        if getattr(obj, "ndim", 0) > 0:
            return [_sanitize(x) for x in obj.tolist()]
        val = obj.item()
        return _sanitize(val)
    if hasattr(obj, "item"):
        val = obj.item()
        return _sanitize(val)

    # Recurse into dicts and lists
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(x) for x in obj]

    return obj


class BaseHandler:
    """
    Base class for API handlers.

    Provides access to service components and common utilities.
    """

    def __init__(self, service: "InterlaceService"):
        self.service = service

    @property
    def config(self) -> Any:
        """Get service configuration."""
        return self.service.config

    @property
    def models(self) -> dict[str, dict[str, Any]]:
        """Get discovered models."""
        return self.service.models

    @property
    def graph(self) -> Any:
        """Get dependency graph."""
        return self.service.graph

    @property
    def state_store(self) -> Any:
        """Get state store for historical data."""
        return self.service.state_store

    @property
    def event_bus(self) -> Any:
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
        return web.json_response(_sanitize(data), status=status, headers=headers)
