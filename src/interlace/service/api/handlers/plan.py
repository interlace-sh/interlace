"""
Impact analysis / plan preview API endpoints.

Provides endpoints for previewing what will happen during execution.
"""

import json

from aiohttp import web

from interlace.core.impact import ImpactAnalyzer
from interlace.service.api.errors import ValidationError
from interlace.service.api.handlers import BaseHandler
from interlace.utils.logging import get_logger

logger = get_logger("interlace.api.plan")


class PlanHandler(BaseHandler):
    """Handler for plan/impact analysis endpoints."""

    async def get_plan(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/plan

        Preview what will happen during execution.

        Query params:
            models: Comma-separated list of models (default: all)
            force: If true, treat all models as needing to run
        """
        # Parse query params
        models_param = request.query.get("models")
        models = None
        if models_param:
            models = [m.strip() for m in models_param.split(",") if m.strip()]

        force = request.query.get("force", "").lower() in ("true", "1", "yes")

        # Create change detector if available
        change_detector = None
        try:
            from interlace.core.execution.change_detector import ChangeDetector

            change_detector = ChangeDetector(self.models, self.state_store)
        except Exception as e:
            logger.debug(f"Could not initialize change detector: {e}")

        # Create impact analyzer
        analyzer = ImpactAnalyzer(
            models=self.models,
            graph=self.graph,
            change_detector=change_detector,
            state_store=self.state_store,
        )

        # Analyze
        result = analyzer.analyze(target_models=models, force=force)

        return await self.json_response(result.to_dict(), request=request)

    async def post_plan(self, request: web.Request) -> web.Response:
        """
        POST /api/v1/plan

        Preview what will happen during execution.

        Body:
            models: List of model names (default: all)
            force: If true, treat all models as needing to run
        """
        try:
            body = await request.json()
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON in request body: {e.msg}") from e
        except Exception:
            # Empty body is valid for this endpoint (all models will be planned)
            body = {}

        models = body.get("models")
        force = body.get("force", False)

        # Create change detector if available
        change_detector = None
        try:
            from interlace.core.execution.change_detector import ChangeDetector

            change_detector = ChangeDetector(self.models, self.state_store)
        except Exception as e:
            logger.debug(f"Could not initialize change detector: {e}")

        # Create impact analyzer
        analyzer = ImpactAnalyzer(
            models=self.models,
            graph=self.graph,
            change_detector=change_detector,
            state_store=self.state_store,
        )

        # Analyze
        result = analyzer.analyze(target_models=models, force=force)

        return await self.json_response(result.to_dict(), request=request)
