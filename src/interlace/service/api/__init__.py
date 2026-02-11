"""
REST API module for Interlace.

Provides HTTP endpoints for the admin Web UI and programmatic access.
"""

from interlace.service.api.routes import setup_routes
from interlace.service.api.events import EventBus

__all__ = ["setup_routes", "EventBus"]
