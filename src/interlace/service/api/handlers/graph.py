"""
Dependency graph endpoints.
"""

from typing import Any

from aiohttp import web

from interlace.service.api.errors import ValidationError
from interlace.service.api.handlers import BaseHandler


class GraphHandler(BaseHandler):
    """Handler for dependency graph endpoints."""

    async def get(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/graph

        Get the full dependency graph for visualization.

        Query params:
            format: "full" (default), "layers", or "tree"
            models: Comma-separated list of models to include (default: all)

        Returns nodes, edges, and layer information suitable for DAG rendering.
        """
        format_type = request.query.get("format", "full")
        models_filter = request.query.get("models")

        if models_filter:
            models_filter = [m.strip() for m in models_filter.split(",")]
            # Validate all requested models exist
            invalid_models = [m for m in models_filter if m not in self.models]
            if invalid_models:
                raise ValidationError(
                    f"Unknown models: {invalid_models}",
                    details={
                        "invalid_models": invalid_models,
                        "available_models": list(self.models.keys())[:20],
                    },
                )

        # Build graph data
        nodes = []
        edges = []
        layers = {}

        # Get layer information
        if self.graph:
            layers = self.graph.get_layers()

        # Build nodes
        for name, model in self.models.items():
            if models_filter and name not in models_filter:
                continue

            layer = layers.get(name, 0)
            nodes.append(
                {
                    "id": name,
                    "name": name,
                    "type": model.get("type", "python"),
                    "schema": model.get("schema", "public"),
                    "materialise": model.get("materialise", "table"),
                    "strategy": model.get("strategy"),
                    "layer": layer,
                    "tags": model.get("tags", []),
                    "description": model.get("description", ""),
                }
            )

        # Build edges from dependencies
        for name in self.models:
            if models_filter and name not in models_filter:
                continue

            if self.graph:
                deps = self.graph.get_dependencies(name)
                for dep in deps:
                    # Only include edge if both nodes are in filter
                    if models_filter and dep not in models_filter:
                        continue
                    edges.append(
                        {
                            "id": f"{dep}->{name}",
                            "source": dep,
                            "target": name,
                        }
                    )

        # Group nodes by layer for visualization hints
        layers_grouped: dict[int, list[str]] = {}
        for node in nodes:
            layer = node["layer"]
            if layer not in layers_grouped:
                layers_grouped[layer] = []
            layers_grouped[layer].append(node["id"])

        response_data = {
            "nodes": nodes,
            "edges": edges,
            "layers": layers_grouped,
            "stats": {
                "total_nodes": len(nodes),
                "total_edges": len(edges),
                "max_layer": max(layers_grouped.keys()) if layers_grouped else 0,
            },
        }

        # Add format-specific data
        if format_type == "tree" and self.graph:
            response_data["tree"] = self._build_tree_structure()
        elif format_type == "layers":
            response_data["nodes_by_layer"] = [
                {
                    "layer": layer,
                    "nodes": [n for n in nodes if n["layer"] == layer],
                }
                for layer in sorted(layers_grouped.keys())
            ]

        return await self.json_response(response_data, request=request)

    async def validate(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/graph/validate

        Validate the dependency graph for cycles and issues.
        """
        issues = []

        if self.graph:
            # Check for cycles
            cycles = self.graph.detect_cycles()
            if cycles:
                for cycle in cycles:
                    issues.append(
                        {
                            "type": "cycle",
                            "severity": "error",
                            "message": f"Circular dependency detected: {' -> '.join(cycle)}",
                            "models": cycle,
                        }
                    )

            # Check for missing dependencies
            for name in self.models:
                deps = self.graph.get_dependencies(name)
                for dep in deps:
                    if dep not in self.models:
                        issues.append(
                            {
                                "type": "missing_dependency",
                                "severity": "warning",
                                "message": f"Model '{name}' depends on '{dep}' which is not defined",
                                "models": [name, dep],
                            }
                        )

        return await self.json_response(
            {
                "valid": len([i for i in issues if i["severity"] == "error"]) == 0,
                "issues": issues,
            },
            request=request,
        )

    def _build_tree_structure(self) -> list[dict[str, Any]]:
        """Build tree structure from graph for tree visualization."""
        # Find root nodes (no dependencies)
        roots = []
        for name in self.models:
            if self.graph:
                deps = self.graph.get_dependencies(name)
                if not deps:
                    roots.append(name)

        def build_subtree(name: str, visited: set) -> dict[str, Any]:
            if name in visited:
                return {"id": name, "name": name, "children": [], "cyclic": True}

            visited = visited | {name}
            children = []

            if self.graph:
                dependents = self.graph.get_dependents(name)
                for dep in dependents:
                    children.append(build_subtree(dep, visited))

            return {
                "id": name,
                "name": name,
                "type": self.models.get(name, {}).get("type", "unknown"),
                "children": children,
            }

        return [build_subtree(root, set()) for root in roots]
