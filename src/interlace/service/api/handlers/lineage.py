"""
Column-level lineage API endpoints.

Provides endpoints for querying and refreshing column-level lineage.
"""

from typing import Any

from aiohttp import web

from interlace.lineage import (
    ColumnLineage,
    IbisLineageExtractor,
    LineageGraph,
    SqlLineageExtractor,
)
from interlace.service.api.errors import ErrorCode, NotFoundError, ValidationError
from interlace.service.api.handlers import BaseHandler
from interlace.utils.logging import get_logger

logger = get_logger("interlace.api.lineage")


class LineageHandler(BaseHandler):
    """Handler for column-level lineage endpoints."""

    def __init__(self, service: Any) -> None:
        super().__init__(service)
        self._lineage_cache: dict[str, ColumnLineage] = {}
        self._lineage_graph: LineageGraph | None = None

    def _get_extractor(self, model_type: str) -> Any:
        """Get the appropriate lineage extractor."""
        if model_type == "sql":
            return SqlLineageExtractor()
        return IbisLineageExtractor()

    def _compute_model_lineage(self, model_name: str) -> ColumnLineage | None:
        """Compute lineage for a single model."""
        if model_name in self._lineage_cache:
            return self._lineage_cache[model_name]

        if model_name not in self.models:
            return None

        model_info = self.models[model_name]
        model_type = model_info.get("type", "python")

        try:
            extractor = self._get_extractor(model_type)
            lineage = extractor.extract(model_name, model_info, self.models)
            self._lineage_cache[model_name] = lineage
            return lineage  # type: ignore[no-any-return]
        except Exception as e:
            logger.debug(f"Could not compute lineage for {model_name}: {e}")
            return None

    def _get_lineage_graph(self) -> LineageGraph:
        """Get or build the full lineage graph."""
        if self._lineage_graph is not None:
            return self._lineage_graph

        self._lineage_graph = LineageGraph()

        for model_name in self.models:
            lineage = self._compute_model_lineage(model_name)
            if lineage:
                self._lineage_graph.add_model_lineage(lineage)

        return self._lineage_graph

    async def get_full_lineage(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/lineage

        Get the full column-level lineage graph.

        Query params:
            models: Comma-separated list of models to include (default: all)
        """
        models_filter = request.query.get("models")
        if models_filter:
            model_names = [m.strip() for m in models_filter.split(",")]
        else:
            model_names = list(self.models.keys())

        # Build lineage for requested models
        result: dict[str, Any] = {
            "models": {},
            "edges": [],
        }

        edges_set = set()

        for model_name in model_names:
            lineage = self._compute_model_lineage(model_name)
            if lineage:
                result["models"][model_name] = {
                    "columns": [
                        {
                            "name": col.name,
                            "data_type": col.data_type,
                            "sources_count": len(col.sources),
                        }
                        for col in lineage.columns
                    ]
                }

                # Collect edges
                for col in lineage.columns:
                    for edge in col.sources:
                        edge_key = (
                            edge.source_model,
                            edge.source_column,
                            edge.output_model,
                            col.name,
                        )
                        if edge_key not in edges_set:
                            edges_set.add(edge_key)
                            result["edges"].append(
                                {
                                    "source_model": edge.source_model,
                                    "source_column": edge.source_column,
                                    "output_model": edge.output_model,
                                    "output_column": col.name,
                                    "transformation_type": edge.transformation_type.value,
                                }
                            )

        return await self.json_response(result, request=request)

    async def get_model_columns(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/models/{name}/columns

        Get all columns for a model with their lineage information.
        """
        name = request.match_info["name"]

        if name not in self.models:
            raise NotFoundError("Model", name, ErrorCode.MODEL_NOT_FOUND)

        lineage = self._compute_model_lineage(name)

        if lineage is None:
            return await self.json_response(
                {"model": name, "columns": [], "error": "Could not compute lineage"},
                request=request,
            )

        columns = []
        for col in lineage.columns:
            col_data = {
                "name": col.name,
                "data_type": col.data_type,
                "nullable": col.nullable,
                "is_primary_key": col.is_primary_key,
                "description": col.description,
                "sources": [
                    {
                        "model": edge.source_model,
                        "column": edge.source_column,
                        "transformation": edge.transformation_type.value,
                    }
                    for edge in col.sources
                ],
            }
            columns.append(col_data)

        return await self.json_response(
            {"model": name, "columns": columns},
            request=request,
        )

    async def get_column_lineage(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/models/{name}/columns/{column}/lineage

        Get detailed lineage for a specific column.

        Query params:
            direction: "upstream", "downstream", or "both" (default: "upstream")
            depth: Max depth to traverse (default: 3)
        """
        model_name = request.match_info["name"]
        column_name = request.match_info["column"]

        if model_name not in self.models:
            raise NotFoundError("Model", model_name, ErrorCode.MODEL_NOT_FOUND)

        direction = request.query.get("direction", "upstream")

        # Parse and validate depth parameter
        try:
            depth = int(request.query.get("depth", 3))
            depth = max(0, min(depth, 100))  # Limit depth to prevent DoS
        except (ValueError, TypeError) as e:
            raise ValidationError("'depth' must be a valid integer between 0 and 100") from e

        lineage = self._compute_model_lineage(model_name)
        if lineage is None:
            return await self.json_response(
                {"error": "Could not compute lineage"},
                status=500,
                request=request,
            )

        col = lineage.get_column(column_name)
        if col is None:
            raise NotFoundError("Column", column_name, ErrorCode.RESOURCE_NOT_FOUND)

        result: dict[str, Any] = {
            "model": model_name,
            "column": column_name,
            "data_type": col.data_type,
            "sources": [],
            "dependents": [],
        }

        # Build full graph for tracing
        graph = self._get_lineage_graph()

        # Get upstream lineage
        if direction in ("upstream", "both"):
            upstream_edges = graph.trace_column_upstream(model_name, column_name, depth)
            result["sources"] = [
                {
                    "source_model": edge.source_model,
                    "source_column": edge.source_column,
                    "output_model": edge.output_model,
                    "output_column": edge.output_column,
                    "transformation": edge.transformation_type.value,
                }
                for edge in upstream_edges
            ]

        # Get downstream lineage
        if direction in ("downstream", "both"):
            downstream_edges = graph.trace_column_downstream(model_name, column_name, depth)
            result["dependents"] = [
                {
                    "source_model": edge.source_model,
                    "source_column": edge.source_column,
                    "output_model": edge.output_model,
                    "output_column": edge.output_column,
                    "transformation": edge.transformation_type.value,
                }
                for edge in downstream_edges
            ]

        return await self.json_response(result, request=request)

    async def refresh_lineage(self, request: web.Request) -> web.Response:
        """
        POST /api/v1/lineage/refresh

        Recompute and cache column lineage for all models.

        Body (optional):
            models: List of model names to refresh (default: all)
        """
        try:
            body = await request.json()
        except Exception:
            body = {}

        models_to_refresh = body.get("models")
        if models_to_refresh is None:
            models_to_refresh = list(self.models.keys())

        # Clear cache for specified models
        for model_name in models_to_refresh:
            self._lineage_cache.pop(model_name, None)

        # Reset full graph
        self._lineage_graph = None

        # Recompute lineage
        success_count = 0
        error_count = 0
        errors = []

        for model_name in models_to_refresh:
            try:
                lineage = self._compute_model_lineage(model_name)
                if lineage:
                    # Also persist to state store if available
                    if self.state_store:
                        self.state_store.clear_model_lineage(model_name)
                        for col in lineage.columns:
                            self.state_store.save_model_column(
                                model_name=model_name,
                                column_name=col.name,
                                data_type=col.data_type,
                                is_nullable=col.nullable,
                                is_primary_key=col.is_primary_key,
                                description=col.description,
                            )
                            for edge in col.sources:
                                self.state_store.save_column_lineage(
                                    output_model=model_name,
                                    output_column=col.name,
                                    source_model=edge.source_model,
                                    source_column=edge.source_column,
                                    transformation_type=edge.transformation_type.value,
                                    transformation_expression=edge.transformation_expression,
                                    confidence=edge.confidence,
                                )
                    success_count += 1
                else:
                    error_count += 1
                    errors.append({"model": model_name, "error": "No lineage computed"})
            except Exception as e:
                error_count += 1
                errors.append({"model": model_name, "error": str(e)})
                logger.debug(f"Error refreshing lineage for {model_name}", exc_info=True)

        return await self.json_response(
            {
                "success": success_count,
                "errors": error_count,
                "error_details": errors if errors else None,
            },
            request=request,
        )
