"""
Tests for core functionality.

Phase 0: Basic tests for model decorator and dependencies.
"""

from interlace.core.model import model
from interlace.core.dependencies import DependencyGraph


def test_model_decorator():
    """Test that @model decorator sets metadata."""

    @model(name="test_model", schema="public", materialize="table")
    def test_model():
        return None

    assert hasattr(test_model, "_interlace_model")
    assert test_model._interlace_model["name"] == "test_model"
    assert test_model._interlace_model["schema"] == "public"


def test_dependency_graph():
    """Test dependency graph building."""
    graph = DependencyGraph()
    graph.add_model("a", [])
    graph.add_model("b", ["a"])
    graph.add_model("c", ["a", "b"])

    assert graph.get_dependencies("c") == ["a", "b"]
    assert graph.get_dependents("a") == ["b", "c"]

    # Topological sort should respect dependencies
    sorted_models = graph.topological_sort()
    assert sorted_models.index("a") < sorted_models.index("b")
    assert sorted_models.index("b") < sorted_models.index("c")
