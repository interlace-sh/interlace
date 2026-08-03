"""The dependency DAG.

A directed acyclic graph of model names with edges pointing from a node to its
upstreams. Kahn's algorithm gives a deterministic topological order (upstreams
before downstreams); a short order means a cycle. No third-party graph library —
this is ~40 lines of stdlib.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable

from interlace.exceptions import DependencyError


class DependencyGraph:
    """Model names and their upstream dependencies."""

    def __init__(self) -> None:
        self._upstreams: dict[str, set[str]] = {}

    def add_node(self, name: str) -> None:
        self._upstreams.setdefault(name, set())

    def add_dependency(self, node: str, upstream: str) -> None:
        """Record that ``node`` depends on ``upstream``. Self-edges are ignored."""
        self.add_node(node)
        self.add_node(upstream)
        if node != upstream:
            self._upstreams[node].add(upstream)

    def upstreams(self, node: str) -> set[str]:
        return set(self._upstreams.get(node, set()))

    def downstreams(self, node: str) -> set[str]:
        return {n for n, ups in self._upstreams.items() if node in ups}

    def ancestors(self, node: str) -> set[str]:
        """All transitive upstreams of ``node``."""
        return self._reach(node, self.upstreams)

    def descendants(self, node: str) -> set[str]:
        """All transitive downstreams of ``node``."""
        return self._reach(node, self.downstreams)

    def _reach(self, node: str, step: Callable[[str], set[str]]) -> set[str]:
        result: set[str] = set()
        stack = list(step(node))
        while stack:
            current = stack.pop()
            if current not in result:
                result.add(current)
                stack.extend(step(current))
        return result

    def topological_sort(self) -> list[str]:
        """Return nodes ordered upstreams-first. Raises ``DependencyError`` on a cycle."""
        indegree = {n: len(ups) for n, ups in self._upstreams.items()}
        adjacency: dict[str, list[str]] = {n: [] for n in self._upstreams}
        for node, ups in self._upstreams.items():
            for upstream in ups:
                adjacency[upstream].append(node)

        queue = deque(sorted(n for n, deg in indegree.items() if deg == 0))
        order: list[str] = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for downstream in sorted(adjacency[node]):
                indegree[downstream] -= 1
                if indegree[downstream] == 0:
                    queue.append(downstream)

        if len(order) != len(self._upstreams):
            cyclic = sorted(set(self._upstreams) - set(order))
            raise DependencyError("dependency cycle detected", details={"nodes": cyclic})
        return order
