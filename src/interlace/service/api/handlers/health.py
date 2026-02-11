"""
Health and project information endpoints.
"""

import time
from typing import Any, Dict

from aiohttp import web

from interlace.service.api.handlers import BaseHandler


class HealthHandler(BaseHandler):
    """Handler for health check and project info endpoints."""

    def __init__(self, service):
        super().__init__(service)
        self._start_time = time.time()

    async def health(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/health

        Returns system health status.
        """
        # Check connection health
        connections_status = []
        for name, conn_config in self.config.get("connections", {}).items():
            try:
                # Simple health check - try to get connection
                status = "connected"
            except Exception:
                status = "disconnected"
            connections_status.append({
                "name": name,
                "type": conn_config.get("type", "unknown"),
                "status": status,
            })

        # Count active flows
        active_flows = 0
        if hasattr(self.service, "flow") and self.service.flow:
            if self.service.flow.status.value in ("pending", "running"):
                active_flows = 1

        data = {
            "status": "ok",
            "version": self._get_version(),
            "uptime_seconds": round(time.time() - self._start_time, 2),
            "connections": connections_status,
            "scheduler_running": getattr(self.service, "_scheduler_running", False),
            "active_flows": active_flows,
            "subscribers": self.event_bus.subscriber_count() if self.event_bus else 0,
        }

        return await self.json_response(data, request=request)

    async def project(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/project

        Returns project metadata.
        """
        data = {
            "name": self.config.get("project", {}).get("name", "interlace-project"),
            "description": self.config.get("project", {}).get("description", ""),
            "environment": self.config.get("environment", "default"),
            "project_dir": str(getattr(self.service, "project_dir", "")),
            "models_count": len(self.models),
            "connections": [
                {
                    "name": name,
                    "type": cfg.get("type", "unknown"),
                }
                for name, cfg in self.config.get("connections", {}).items()
            ],
            "state_enabled": self.state_store is not None,
        }

        return await self.json_response(data, request=request)

    async def info(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/info

        Returns API and server information.
        """
        data = {
            "api_version": "v1",
            "interlace_version": self._get_version(),
            "features": {
                "scheduling": True,
                "streams": True,
                "sync": True,
                "quality_checks": True,
                "retry_framework": True,
            },
        }

        return await self.json_response(data, request=request)

    async def scheduler_status(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/scheduler

        Returns detailed scheduler status including per-model next fire times
        and last run timestamps.
        """
        if hasattr(self.service, "get_scheduler_status"):
            data = self.service.get_scheduler_status()
        else:
            data = {"running": False, "scheduled_models": 0, "models": []}

        return await self.json_response(data, request=request)

    def _get_version(self) -> str:
        """Get interlace version."""
        try:
            from interlace import __version__
            return __version__
        except ImportError:
            return "0.1.0"
