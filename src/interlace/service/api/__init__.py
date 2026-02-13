"""
REST API module for Interlace.

Provides HTTP endpoints for the admin Web UI and programmatic access.
"""

from interlace.service.api.events import EventBus
from interlace.service.api.routes import setup_routes

__all__ = ["setup_routes", "EventBus"]
