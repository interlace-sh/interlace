"""
Core engine for model execution and dependency management.

Phase 0: Basic model decorator, dependency graph, and dynamic execution.
"""

from interlace.core.model import model
from interlace.core.executor import Executor, execute_models
from interlace.core.dependencies import build_dependency_graph, DependencyGraph

__all__ = [
    "model",
    "Executor",
    "execute_models",
    "build_dependency_graph",
    "DependencyGraph",
]
