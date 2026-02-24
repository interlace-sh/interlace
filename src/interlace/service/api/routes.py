"""
API route registration.

Registers all API endpoints with versioned prefix.
"""

from typing import TYPE_CHECKING

from aiohttp import web

from interlace.service.api.handlers.docs import openapi_yaml, swagger_ui
from interlace.service.api.handlers.events import EventsHandler
from interlace.service.api.handlers.flows import FlowsHandler
from interlace.service.api.handlers.graph import GraphHandler
from interlace.service.api.handlers.health import HealthHandler
from interlace.service.api.handlers.lineage import LineageHandler
from interlace.service.api.handlers.models import ModelsHandler
from interlace.service.api.handlers.plan import PlanHandler
from interlace.service.api.handlers.runs import RunsHandler
from interlace.service.api.handlers.schema import SchemaHandler
from interlace.service.api.handlers.streams import StreamsHandler

if TYPE_CHECKING:
    from interlace.service.server import InterlaceService


def setup_routes(app: web.Application, service: "InterlaceService") -> None:
    """
    Register all API routes.

    Args:
        app: aiohttp Application
        service: InterlaceService instance for handler access
    """
    # Instantiate handlers
    health = HealthHandler(service)
    models = ModelsHandler(service)
    flows = FlowsHandler(service)
    runs = RunsHandler(service)
    graph = GraphHandler(service)
    events = EventsHandler(service)
    lineage = LineageHandler(service)
    plan = PlanHandler(service)
    schema = SchemaHandler(service)
    streams = StreamsHandler(service)

    # API version prefix
    prefix = "/api/v1"

    # API documentation (outside versioned prefix)
    app.router.add_routes(
        [
            web.get("/api/docs", swagger_ui),
            web.get("/api/openapi.yaml", openapi_yaml),
        ]
    )

    # Register routes
    app.router.add_routes(
        [
            # Health & Info
            web.get(f"{prefix}/health", health.health),
            web.get(f"{prefix}/project", health.project),
            web.get(f"{prefix}/info", health.info),
            web.get(f"{prefix}/scheduler", health.scheduler_status),
            # Models
            web.get(f"{prefix}/models", models.list),
            web.get(f"{prefix}/models/{{name}}", models.get),
            web.get(f"{prefix}/models/{{name}}/lineage", models.lineage),
            web.get(f"{prefix}/models/{{name}}/runs", models.runs),
            # Flows (execution history)
            web.get(f"{prefix}/flows", flows.list),
            web.get(f"{prefix}/flows/{{flow_id}}", flows.get),
            web.get(f"{prefix}/flows/{{flow_id}}/tasks", flows.tasks),
            # Runs (trigger execution)
            web.post(f"{prefix}/runs", runs.create),
            web.post(f"{prefix}/runs/{{run_id}}/cancel", runs.cancel),
            web.get(f"{prefix}/runs/{{run_id}}/status", runs.status),
            # Graph
            web.get(f"{prefix}/graph", graph.get),
            web.get(f"{prefix}/graph/validate", graph.validate),
            # SSE Events
            web.get(f"{prefix}/events", events.stream),
            # Column Lineage
            web.get(f"{prefix}/lineage", lineage.get_full_lineage),
            web.post(f"{prefix}/lineage/refresh", lineage.refresh_lineage),
            web.get(f"{prefix}/models/{{name}}/columns", lineage.get_model_columns),
            web.get(f"{prefix}/models/{{name}}/columns/{{column}}/lineage", lineage.get_column_lineage),
            # Plan / Impact Analysis
            web.get(f"{prefix}/plan", plan.get_plan),
            web.post(f"{prefix}/plan", plan.post_plan),
            # Schema History
            web.get(f"{prefix}/models/{{name}}/schema/history", schema.get_history),
            web.get(f"{prefix}/models/{{name}}/schema/current", schema.get_current_version),
            web.get(f"{prefix}/models/{{name}}/schema/diff", schema.compare_versions),
            # Streams (webhook/event ingestion)
            web.get(f"{prefix}/streams", streams.list),
            web.get(f"{prefix}/streams/{{name}}", streams.get),
            web.post(f"{prefix}/streams/{{name}}", streams.publish),
            web.get(f"{prefix}/streams/{{name}}/subscribe", streams.subscribe_sse),
            web.post(f"{prefix}/streams/{{name}}/consume", streams.consume_batch),
            web.post(f"{prefix}/streams/{{name}}/ack", streams.ack_events),
        ]
    )


def setup_legacy_routes(app: web.Application, service: "InterlaceService") -> None:
    """
    Setup legacy routes for backward compatibility.

    These routes maintain compatibility with existing integrations
    while the new /api/v1/* routes are preferred.
    """
    # Keep existing /health at root for simple health checks
    health = HealthHandler(service)
    app.router.add_get("/health", health.health)
