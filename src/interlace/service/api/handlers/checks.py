"""
Check endpoints.

Provides API access to data check configurations, results, and execution.
"""

from __future__ import annotations

from typing import Any

from aiohttp import web

from interlace.service.api.handlers import BaseHandler


class ChecksHandler(BaseHandler):
    """Handler for check endpoints."""

    async def list_all(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/checks

        List all checks across all models.
        """
        models = self.service.models or {}
        all_checks: list[dict[str, Any]] = []

        for model_name, model_info in models.items():
            checks = model_info.get("checks", [])
            for check_config in checks:
                if isinstance(check_config, dict):
                    entry = {**check_config, "model": model_name}
                else:
                    # Check instance — serialise basics
                    entry = {
                        "type": check_config.check_type,
                        "name": check_config.check_name,
                        "severity": check_config.severity.value,
                        "model": model_name,
                    }
                all_checks.append(entry)

        return await self.json_response({"checks": all_checks, "total": len(all_checks)}, request=request)

    async def list_for_model(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/models/{name}/checks

        List checks configured for a specific model.
        """
        model_name = request.match_info["name"]
        models = self.service.models or {}

        if model_name not in models:
            return await self.json_response({"error": f"Model '{model_name}' not found"}, status=404, request=request)

        model_info = models[model_name]
        checks = model_info.get("checks", [])
        check_list: list[dict[str, Any]] = []

        for check_config in checks:
            if isinstance(check_config, dict):
                check_list.append(check_config)
            else:
                check_list.append(
                    {
                        "type": check_config.check_type,
                        "name": check_config.check_name,
                        "severity": check_config.severity.value,
                    }
                )

        return await self.json_response({"model": model_name, "checks": check_list}, request=request)

    async def results(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/checks/results

        Query check results with optional filters.

        Query params:
            model: Filter by model name
            status: Filter by status (passed, failed, error, skipped)
            limit: Max results (default: 50)
        """
        state_store = self.service.state_store
        if not state_store:
            return await self.json_response({"results": [], "total": 0}, request=request)

        try:
            conn = state_store._get_connection()
            if conn is None:
                return await self.json_response({"results": [], "total": 0}, request=request)

            model_filter = request.query.get("model")
            status_filter = request.query.get("status")
            limit = min(int(request.query.get("limit", 50)), 1000)

            query = "SELECT * FROM interlace.check_results"
            conditions: list[str] = []
            if model_filter:
                conditions.append(f"model_name = '{model_filter}'")
            if status_filter:
                conditions.append(f"status = '{status_filter}'")
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += f" ORDER BY executed_at DESC LIMIT {limit}"

            result_table = conn.sql(query)
            df = result_table.execute()
            results = df.to_dict(orient="records")

            return await self.json_response({"results": results, "total": len(results)}, request=request)
        except Exception:
            return await self.json_response({"results": [], "total": 0}, request=request)

    async def run(self, request: web.Request) -> web.Response:
        """
        POST /api/v1/checks/run

        Trigger check execution for all or specific models.

        Request body (JSON, optional):
            {"models": ["model_a", "model_b"]}
        """

        from interlace.core.checks_runner import run_checks_only

        models = self.service.models or {}
        config: dict[str, Any] = self.service.config or {}

        # Optionally filter to specific models
        try:
            body = await request.json()
            model_names = body.get("models")
            if model_names:
                models = {k: v for k, v in models.items() if k in model_names}
        except Exception:
            pass  # No body or invalid JSON — run all

        try:
            results = await run_checks_only(models, config)
            return await self.json_response({"results": results}, request=request)
        except Exception as e:
            return await self.json_response({"error": str(e)}, status=500, request=request)
