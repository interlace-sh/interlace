"""
Dependency graph building and management.

Phase 0: Build dependency graph from model dependencies (implicit/explicit).
"""

from typing import Dict, List, Set, Optional, Any
from collections import defaultdict, deque


class DependencyGraph:
    """Represents a directed acyclic graph of model dependencies."""

    def __init__(self):
        self._graph: Dict[str, List[str]] = defaultdict(list)  # model -> dependencies
        self._reverse: Dict[str, List[str]] = defaultdict(list)  # model -> dependents

    def add_model(self, model_name: str, dependencies: Optional[List[str]] = None):
        """Add a model and its dependencies to the graph."""
        dependencies = dependencies or []

        # Remove stale reverse edges from previous dependencies
        old_deps = self._graph.get(model_name, [])
        for old_dep in old_deps:
            try:
                self._reverse[old_dep].remove(model_name)
            except ValueError:
                pass  # Already removed or never existed

        self._graph[model_name] = dependencies

        # Build reverse graph for dependents
        for dep in dependencies:
            if model_name not in self._reverse[dep]:
                self._reverse[dep].append(model_name)

    def get_dependencies(self, model_name: str) -> List[str]:
        """Get dependencies for a model."""
        return self._graph.get(model_name, [])

    def get_dependents(self, model_name: str) -> List[str]:
        """Get dependents (models that depend on this one)."""
        return self._reverse.get(model_name, [])

    def topological_sort(self) -> List[str]:
        """
        Topological sort of models by dependencies.

        Returns models in execution order (dependencies before dependents).
        """
        in_degree: Dict[str, int] = defaultdict(int)

        # Calculate in-degrees
        for model_name in self._graph:
            in_degree[model_name] = 0

        for model_name, deps in self._graph.items():
            for dep in deps:
                if dep in self._graph:  # Only count dependencies that are models
                    in_degree[model_name] += 1

        # Kahn's algorithm
        queue = deque([model for model, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            model_name = queue.popleft()
            result.append(model_name)

            # Decrease in-degree for dependents
            for dependent in self._reverse[model_name]:
                if dependent in self._graph:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

        return result

    def detect_cycles(self) -> List[List[str]]:
        """
        Detect cycles in the dependency graph.

        Uses DFS to find all cycles. The rec_stack and path are always
        properly maintained so that non-cyclic nodes sharing edges with
        cyclic nodes are not falsely reported.
        """
        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        cycles: List[List[str]] = []
        path: List[str] = []

        def dfs(node: str):
            """DFS helper to detect cycles."""
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in self._graph.get(node, []):
                if neighbor not in visited:
                    dfs(neighbor)
                elif neighbor in rec_stack:
                    # Found a cycle: from neighbor back to neighbor through path
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    cycles.append(cycle)

            rec_stack.remove(node)
            path.pop()

        for node in self._graph:
            if node not in visited:
                dfs(node)

        return cycles

    def get_layers(self) -> Dict[str, int]:
        """
        Get layer (execution level) for each model.

        Returns a dictionary mapping model_name -> layer_number (0-based).
        Models in the same layer can run in parallel.
        """
        in_degree: Dict[str, int] = defaultdict(int)
        model_layers: Dict[str, int] = {}

        # Calculate in-degrees
        for model_name in self._graph:
            in_degree[model_name] = 0

        for model_name, deps in self._graph.items():
            for dep in deps:
                if dep in self._graph:
                    in_degree[model_name] += 1

        # Build layers level by level
        remaining = set(self._graph.keys())
        level = 0

        while remaining:
            # Get all models with no remaining dependencies (ready for this level)
            ready = [m for m in remaining if in_degree[m] == 0]
            if not ready:
                # Cycle or missing dependencies - should not happen if validated
                break

            # Assign layer to ready models
            for model_name in ready:
                model_layers[model_name] = level

            # Decrease in-degree for dependents and remove ready models
            for model_name in ready:
                for dependent in self._reverse[model_name]:
                    if dependent in remaining:
                        in_degree[dependent] -= 1
                remaining.remove(model_name)

            level += 1

        return model_layers

    def visualize_layers(self) -> str:
        """
        Visualize dependency graph as layers (execution levels).

        Returns a string representation showing models grouped by execution level.
        Models in the same layer can run in parallel.
        """
        model_layers = self.get_layers()

        # Group models by layer
        layers: Dict[int, List[str]] = defaultdict(list)
        for model_name, layer in model_layers.items():
            layers[layer].append(model_name)

        # Format output
        lines = []
        for layer_num in sorted(layers.keys()):
            layer_models = sorted(layers[layer_num])
            if len(layer_models) == 1:
                lines.append(f"Layer {layer_num}: {layer_models[0]}")
            else:
                # Multiple models in parallel
                lines.append(f"Layer {layer_num}: {' ── '.join(layer_models)}")

        return "\n".join(lines)

    def visualize_tree(self, root: Optional[str] = None) -> str:
        """
        Visualize dependency graph as a tree starting from root (models with no dependencies).

        Shows dependencies with tree branches (│, ├─, └─).
        """
        # Find root models (no dependencies)
        if root:
            roots = [root] if root in self._graph else []
        else:
            roots = [m for m, deps in self._graph.items() if not deps]

        if not roots:
            return "No root models found (all models have dependencies)"

        lines = []

        def build_tree(
            model: str, prefix: str = "", is_last: bool = True, visited: Set[str] = None
        ) -> None:
            if visited is None:
                visited = set()

            # Avoid cycles in visualization
            if model in visited:
                lines.append(f"{prefix}└─ {model} (cyclic reference)")
                return

            visited.add(model)

            # Print current model
            branch = "└─ " if is_last else "├─ "
            lines.append(f"{prefix}{branch}{model}")

            # Get dependents (downstream models)
            dependents = sorted(self.get_dependents(model))

            if dependents:
                extension = "   " if is_last else "│  "
                for i, dep in enumerate(dependents):
                    is_last_dep = i == len(dependents) - 1
                    build_tree(dep, prefix + extension, is_last_dep, visited.copy())

        for i, root_model in enumerate(sorted(roots)):
            if i > 0:
                lines.append("")
            build_tree(root_model)

        return "\n".join(lines)


def build_dependency_graph(models: Dict[str, Any]) -> DependencyGraph:
    """
    Build dependency graph from models.

    Uses discovered dependencies from model metadata (implicit or explicit).

    Args:
        models: Dictionary of model_name -> model_info

    Returns:
        DependencyGraph instance
    """
    graph = DependencyGraph()

    for model_name, model_info in models.items():
        # Get dependencies (from discovery or explicit annotation)
        dependencies = model_info.get("dependencies")

        # Ensure dependencies is a list
        if dependencies is None:
            dependencies = []
        elif not isinstance(dependencies, list):
            dependencies = [dependencies]

        # Filter out dependencies that aren't models (e.g., read_csv, system tables)
        model_names = set(models.keys())
        valid_dependencies = [dep for dep in dependencies if dep in model_names]

        graph.add_model(model_name, valid_dependencies)

    return graph
