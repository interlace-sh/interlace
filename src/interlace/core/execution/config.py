"""
Configuration objects for execution components.

Reduces coupling by using configuration objects instead of many parameters.

Phase 2: Added retry_manager and dlq for retry framework support.
"""

from typing import Dict, Any, Optional, Callable, TYPE_CHECKING
from dataclasses import dataclass
import ibis

if TYPE_CHECKING:
    from interlace.core.flow import Flow
    from interlace.core.execution.change_detector import ChangeDetector
    from interlace.core.execution.connection_manager import TaskConnectionManager
    from interlace.core.execution.dependency_loader import DependencyLoader
    from interlace.core.execution.schema_manager import SchemaManager
    from interlace.core.execution.materialization_manager import MaterializationManager
    from interlace.core.execution.data_converter import DataConverter
    from interlace.materialization.base import Materializer
    from concurrent.futures import ThreadPoolExecutor
    from interlace.core.state import StateStore
    from interlace.utils.display import RichDisplay
    from interlace.core.retry import RetryManager, DeadLetterQueue


@dataclass
class ModelExecutorConfig:
    """Configuration for ModelExecutor to reduce constructor parameters."""

    change_detector: "ChangeDetector"
    connection_manager: "TaskConnectionManager"
    dependency_loader: "DependencyLoader"
    schema_manager: "SchemaManager"
    materialisation_manager: "MaterializationManager"
    data_converter: "DataConverter"
    materialisers: Dict[str, "Materializer"]
    materialised_tables: Dict[str, ibis.Table]
    executor_pool: "ThreadPoolExecutor"
    state_store: Optional["StateStore"] = None
    flow: Optional["Flow"] = None
    display: Optional["RichDisplay"] = None

    # Phase 2: Retry framework components
    retry_manager: Optional["RetryManager"] = None
    dlq: Optional["DeadLetterQueue"] = None

    # Backfill overrides (--since / --until)
    since: Optional[str] = None
    until: Optional[str] = None

    # Function callbacks (to avoid circular dependencies)
    get_row_count_func: Optional[Callable[[Any, str, str, str, ibis.BaseBackend], Optional[int]]] = None
    update_model_last_run_func: Optional[Callable[[str, str], None]] = None
    log_model_end_func: Optional[Callable[[str, Dict[str, Any], bool, float, Optional[int]], None]] = None
    prepare_model_execution_func: Optional[Callable[[str, Dict[str, Any]], tuple]] = None
    store_materialised_table_func: Optional[Callable[[str, ibis.Table, str, str, ibis.BaseBackend], None]] = None


@dataclass
class ExecutionConfig:
    """Configuration for execution settings."""
    
    max_iterations: int = 100
    table_load_delay: float = 0.01
    task_timeout: float = 30.0
    thread_pool_size: Optional[int] = None
    max_schema_cache_size: int = 1000
    max_existence_cache_size: int = 2000
