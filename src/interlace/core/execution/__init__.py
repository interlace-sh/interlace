"""
Execution components for model execution.

Phase 0: Extracted components from Executor class for better maintainability.
"""

from interlace.core.execution.data_converter import DataConverter
from interlace.core.execution.change_detector import ChangeDetector
from interlace.core.execution.connection_manager import TaskConnectionManager, ConnectionManager
from interlace.core.execution.schema_manager import SchemaManager
from interlace.core.execution.dependency_loader import DependencyLoader
from interlace.core.execution.materialization_manager import MaterializationManager
from interlace.core.execution.model_executor import ModelExecutor
from interlace.core.execution.execution_orchestrator import ExecutionOrchestrator
from interlace.core.execution.config import ModelExecutorConfig, ExecutionConfig

__all__ = [
    "DataConverter",
    "ChangeDetector",
    "TaskConnectionManager",
    "ConnectionManager",  # backward-compatible alias
    "SchemaManager",
    "DependencyLoader",
    "MaterializationManager",
    "ModelExecutor",
    "ExecutionOrchestrator",
    "ModelExecutorConfig",
    "ExecutionConfig",
]

