"""Dependency graph: ordering, cycles, neighbours."""

from __future__ import annotations

import pytest

from interlace.exceptions import DependencyError
from interlace.graph.dag import DependencyGraph

pytestmark = pytest.mark.unit


def test_topological_order_places_upstreams_first() -> None:
    g = DependencyGraph()
    g.add_dependency("b", "a")
    g.add_dependency("c", "b")
    assert g.topological_sort() == ["a", "b", "c"]


def test_independent_nodes_sorted_deterministically() -> None:
    g = DependencyGraph()
    for name in ("x", "a", "m"):
        g.add_node(name)
    assert g.topological_sort() == ["a", "m", "x"]


def test_upstreams_and_downstreams() -> None:
    g = DependencyGraph()
    g.add_dependency("c", "a")
    g.add_dependency("c", "b")
    assert g.upstreams("c") == {"a", "b"}
    assert g.downstreams("a") == {"c"}
    assert g.downstreams("c") == set()


def test_cycle_raises() -> None:
    g = DependencyGraph()
    g.add_dependency("a", "b")
    g.add_dependency("b", "a")
    with pytest.raises(DependencyError):
        g.topological_sort()


def test_self_dependency_is_ignored() -> None:
    g = DependencyGraph()
    g.add_dependency("a", "a")
    assert g.topological_sort() == ["a"]
    assert g.upstreams("a") == set()
