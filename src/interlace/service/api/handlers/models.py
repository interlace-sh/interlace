"""
Model catalog endpoints.
"""

from typing import Any

from aiohttp import web

from interlace.service.api.errors import ErrorCode, NotFoundError, ValidationError
from interlace.service.api.handlers import BaseHandler


class ModelsHandler(BaseHandler):
    """Handler for model-related endpoints."""

    async def list(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/models

        List all models with optional filtering.

        Query params:
            schema: Filter by schema name
            type: Filter by model type (python, sql, stream)
            tags: Filter by tags (comma-separated)
            search: Search in model names
        """
        # Get filter params
        schema_filter = request.query.get("schema")
        type_filter = request.query.get("type")
        tags_filter = request.query.get("tags")
        search_filter = request.query.get("search", "").lower()

        tags_list: list[str] | None = None
        if tags_filter:
            tags_list = [t.strip() for t in tags_filter.split(",")]

        # Filter models
        filtered_models = []
        for name, model in self.models.items():
            # Apply filters
            if schema_filter and model.get("schema") != schema_filter:
                continue
            if type_filter and model.get("type") != type_filter:
                continue
            if tags_list:
                model_tags = model.get("tags", [])
                if not any(t in model_tags for t in tags_list):
                    continue
            if search_filter and search_filter not in name.lower():
                continue

            # Build summary
            filtered_models.append(self._model_summary(name, model))

        # Sort by name
        filtered_models.sort(key=lambda m: m["name"])

        return await self.json_response(
            {"models": filtered_models, "total": len(filtered_models)},
            request=request,
        )

    async def get(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/models/{name}

        Get detailed information about a specific model.
        """
        name = request.match_info["name"]

        if name not in self.models:
            raise NotFoundError("Model", name, ErrorCode.MODEL_NOT_FOUND)

        model = self.models[name]
        detail = self._model_detail(name, model)

        return await self.json_response(detail, request=request)

    async def lineage(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/models/{name}/lineage

        Get upstream and downstream dependencies for a model.

        Query params:
            depth: Max depth to traverse (default: 3)
            direction: "upstream", "downstream", or "both" (default: "both")
        """
        name = request.match_info["name"]

        if name not in self.models:
            raise NotFoundError("Model", name, ErrorCode.MODEL_NOT_FOUND)

        # Parse and validate depth parameter
        try:
            depth = int(request.query.get("depth", 3))
            depth = max(0, min(depth, 100))  # Limit depth to prevent DoS
        except (ValueError, TypeError) as e:
            raise ValidationError("'depth' must be a valid integer between 0 and 100") from e

        direction = request.query.get("direction", "both")

        # Collect nodes and edges
        nodes = []
        edges = []
        visited = set()

        def collect_upstream(model_name: str, current_depth: int) -> None:
            if model_name in visited or current_depth > depth:
                return
            visited.add(model_name)

            if model_name in self.models:
                nodes.append(self._node_data(model_name))

            if self.graph:
                deps = self.graph.get_dependencies(model_name)
                for dep in deps:
                    edges.append({"source": dep, "target": model_name})
                    collect_upstream(dep, current_depth + 1)

        def collect_downstream(model_name: str, current_depth: int) -> None:
            if model_name in visited or current_depth > depth:
                return
            visited.add(model_name)

            if model_name in self.models:
                nodes.append(self._node_data(model_name))

            if self.graph:
                dependents = self.graph.get_dependents(model_name)
                for dep in dependents:
                    edges.append({"source": model_name, "target": dep})
                    collect_downstream(dep, current_depth + 1)

        # Collect based on direction
        if direction in ("upstream", "both"):
            collect_upstream(name, 0)
        visited.discard(name)  # Allow re-visiting for downstream
        if direction in ("downstream", "both"):
            collect_downstream(name, 0)

        # Ensure root node is included
        if not any(n["id"] == name for n in nodes):
            nodes.append(self._node_data(name))

        # Deduplicate edges
        unique_edges = list({(e["source"], e["target"]): e for e in edges}.values())

        return await self.json_response(
            {
                "root": name,
                "nodes": nodes,
                "edges": unique_edges,
            },
            request=request,
        )

    def _model_summary(self, name: str, model: dict[str, Any]) -> dict[str, Any]:
        """Build model summary for list view."""
        layer = 0
        if self.graph and hasattr(self.graph, "get_layers"):
            layers = self.graph.get_layers()
            if layers:
                layer = layers.get(name, 0)

        return {
            "name": name,
            "type": model.get("type", "python"),
            "schema": model.get("schema", "public"),
            "materialise": model.get("materialise", "table"),
            "strategy": model.get("strategy"),
            "description": model.get("description", ""),
            "tags": model.get("tags", []),
            "owner": model.get("owner"),
            "layer": layer,
            "has_schedule": model.get("schedule") is not None,
        }

    def _model_detail(self, name: str, model: dict[str, Any]) -> dict[str, Any]:
        """Build full model detail."""
        # Get dependencies
        dependencies = model.get("dependencies", [])
        if self.graph and hasattr(self.graph, "get_dependencies"):
            deps = self.graph.get_dependencies(name)
            if deps is not None:
                dependencies = deps

        # Get dependents
        dependents = []
        if self.graph and hasattr(self.graph, "get_dependents"):
            deps = self.graph.get_dependents(name)
            if deps is not None:
                dependents = deps

        # Get layer
        layer = 0
        if self.graph and hasattr(self.graph, "get_layers"):
            layers = self.graph.get_layers()
            if layers:
                layer = layers.get(name, 0)

        # Build fields dict
        fields = model.get("fields")
        if fields and hasattr(fields, "items"):
            fields = {k: str(v) for k, v in fields.items()}

        # Normalize primary_key to always be a list
        pk = model.get("primary_key")
        if pk is None:
            pk = []
        elif isinstance(pk, str):
            pk = [pk]

        return {
            "name": name,
            "type": model.get("type", "python"),
            "schema": model.get("schema", "public"),
            "connection": model.get("connection", "default"),
            "materialise": model.get("materialise", "table"),
            "strategy": model.get("strategy", "none"),
            "primary_key": pk,
            "dependencies": dependencies,
            "dependents": dependents,
            "description": model.get("description", ""),
            "tags": model.get("tags", []),
            "owner": model.get("owner"),
            "file": model.get("file", ""),
            "file_hash": model.get("file_hash", ""),
            "layer": layer,
            "fields": fields,
            "incremental": model.get("incremental"),
            "schedule": model.get("schedule"),
            "retry_policy": self._serialize_retry_policy(model.get("retry_policy")),
        }

    def _node_data(self, name: str) -> dict[str, Any]:
        """Build node data for graph visualization."""
        model = self.models.get(name, {})
        layer = 0
        if self.graph:
            layers = self.graph.get_layers()
            layer = layers.get(name, 0)

        return {
            "id": name,
            "name": name,
            "type": model.get("type", "unknown"),
            "schema": model.get("schema", "public"),
            "materialise": model.get("materialise", "table"),
            "layer": layer,
        }

    async def runs(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/models/{name}/runs

        Get recent execution history for a specific model.

        Query params:
            limit: Max results (default: 20, max: 100)
        """
        name = request.match_info["name"]

        if name not in self.models:
            raise NotFoundError("Model", name, ErrorCode.MODEL_NOT_FOUND)

        try:
            limit = int(request.query.get("limit", 20))
            limit = max(1, min(limit, 100))
        except (ValueError, TypeError) as e:
            raise ValidationError("'limit' must be a valid integer between 1 and 100") from e

        runs: list[dict[str, Any]] = []

        # Check current active flow
        if hasattr(self.service, "flow") and self.service.flow:
            flow = self.service.flow
            if hasattr(flow, "tasks") and name in flow.tasks:
                runs.append(self._serialize_model_run(flow, flow.tasks[name]))

        # Check flow history (already newest-first)
        for flow in getattr(self.service, "flow_history", []):
            if len(runs) >= limit:
                break
            if hasattr(flow, "tasks") and name in flow.tasks:
                runs.append(self._serialize_model_run(flow, flow.tasks[name]))

        return await self.json_response(
            {"model": name, "runs": runs[:limit], "total": len(runs)},
            request=request,
        )

    def _serialize_model_run(self, flow: Any, task: Any) -> dict[str, Any]:
        """Serialize a flow+task pair into a model run record."""
        duration = None
        if hasattr(task, "get_duration"):
            duration = task.get_duration()

        return {
            "flow_id": flow.flow_id,
            "flow_status": flow.status.value,
            "flow_started_at": flow.started_at,
            "trigger_type": flow.trigger_type,
            "task_status": task.status.value,
            "duration_seconds": duration,
            "rows_processed": task.rows_processed,
            "started_at": task.started_at,
            "error_message": task.error_message,
        }

    def _serialize_retry_policy(self, policy: Any) -> dict[str, Any] | None:
        """Serialize retry policy to dict."""
        if policy is None:
            return None
        if hasattr(policy, "__dict__"):
            return {
                "max_attempts": getattr(policy, "max_attempts", 3),
                "initial_delay": getattr(policy, "initial_delay", 1.0),
                "max_delay": getattr(policy, "max_delay", 60.0),
                "exponential_base": getattr(policy, "exponential_base", 2.0),
            }
        return None
