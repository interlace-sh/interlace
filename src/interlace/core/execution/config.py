"""
Configuration objects for execution components.

Reduces coupling by using configuration objects instead of many parameters.

Phase 2: Added retry_manager and dlq for retry framework support.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

import ibis

if TYPE_CHECKING:
    from concurrent.futures import ThreadPoolExecutor

    from interlace.core.execution.change_detector import ChangeDetector
    from interlace.core.execution.connection_manager import TaskConnectionManager
    from interlace.core.execution.data_converter import DataConverter
    from interlace.core.execution.dependency_loader import DependencyLoader
    from interlace.core.execution.materialization_manager import MaterializationManager
    from interlace.core.execution.schema_manager import SchemaManager
    from interlace.core.flow import Flow
    from interlace.core.retry import DeadLetterQueue, RetryManager
    from interlace.core.state import StateStore
    from interlace.materialization.base import Materializer
    from interlace.utils.display import Display


@dataclass
class ModelExecutorConfig:
    """Configuration for ModelExecutor to reduce constructor parameters."""

    change_detector: "ChangeDetector"
    connection_manager: "TaskConnectionManager"
    dependency_loader: "DependencyLoader"
    schema_manager: "SchemaManager"
    materialisation_manager: "MaterializationManager"
    data_converter: "DataConverter"
    materialisers: dict[str, "Materializer"]
    materialised_tables: dict[str, ibis.Table]
    executor_pool: "ThreadPoolExecutor"
    state_store: Optional["StateStore"] = None
    flow: Optional["Flow"] = None
    display: Optional["Display"] = None

    # Phase 2: Retry framework components
    retry_manager: Optional["RetryManager"] = None
    dlq: Optional["DeadLetterQueue"] = None

    # Backfill overrides (--since / --until)
    since: str | None = None
    until: str | None = None

    # Function callbacks (to avoid circular dependencies)
    get_row_count_func: Callable[..., Any] | None = None
    update_model_last_run_func: Callable[..., Any] | None = None
    log_model_end_func: Callable[..., Any] | None = None
    prepare_model_execution_func: Callable[..., Any] | None = None
    store_materialised_table_func: Callable[..., Any] | None = None


@dataclass
class ExecutionConfig:
    """Configuration for execution settings."""

    max_iterations: int = 100
    table_load_delay: float = 0.01
    task_timeout: float = 30.0
    thread_pool_size: int | None = None
    max_schema_cache_size: int = 1000
    max_existence_cache_size: int = 2000
