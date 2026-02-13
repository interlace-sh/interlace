"""
Model execution engine.

Phase 0: Dynamic parallel execution engine with DuckDB integration.
"""

import asyncio
import inspect
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import ibis
import ibis.expr.datatypes as dt

from interlace.connections.manager import get_connection, init_connections
from interlace.core.context import get_connection as get_context_connection
from interlace.core.context import set_connection
from interlace.core.dependencies import DependencyGraph
from interlace.core.execution.change_detector import ChangeDetector
from interlace.core.execution.config import ModelExecutorConfig
from interlace.core.execution.connection_manager import TaskConnectionManager
from interlace.core.execution.data_converter import DataConverter
from interlace.core.execution.dependency_loader import DependencyLoader
from interlace.core.execution.execution_orchestrator import ExecutionOrchestrator
from interlace.core.execution.materialization_manager import MaterializationManager
from interlace.core.execution.model_executor import ModelExecutor
from interlace.core.execution.schema_manager import SchemaManager
from interlace.core.flow import Flow
from interlace.core.state import StateStore
from interlace.exceptions import ConnectionLockError
from interlace.materialization.ephemeral import EphemeralMaterializer
from interlace.materialization.table import TableMaterializer
from interlace.materialization.view import ViewMaterializer
from interlace.strategies.append import AppendStrategy
from interlace.strategies.merge_by_key import MergeByKeyStrategy
from interlace.strategies.none import NoneStrategy
from interlace.strategies.replace import ReplaceStrategy
from interlace.utils.display import get_display
from interlace.utils.logging import get_logger
from interlace.utils.table_utils import check_table_exists, get_row_count_efficient, try_load_table

logger = get_logger("interlace.executor")


class Executor:
    """
    Executes models with dynamic parallel execution.

    The executor manages model execution with dependency resolution, parallel execution,
    schema validation, and materialization. It uses asyncio for concurrent execution
    and creates per-task database connections to ensure thread-safety.

    Attributes:
        config: Configuration dictionary
        completed: Set of completed model names
        executing: Set of currently executing model names
        results: Dictionary mapping model names to execution results
        materialised_tables: Dictionary mapping model names to materialised ibis.Table objects
        logger: Logger instance
        flow: Flow object tracking execution state
        flow_start_time: Timestamp when flow execution started
        _dep_loading_locks: Dictionary mapping dependency names to asyncio.Lock objects
        _schema_cache: Dictionary caching table schemas for performance
        _table_existence_cache: Dictionary caching table existence checks
        _dep_schema_cache: Dictionary caching dependency schema locations
    """

    # Configuration constants (can be overridden via config)
    DEFAULT_MAX_ITERATIONS = 100  # Maximum loop iterations to prevent infinite loops
    DEFAULT_TABLE_LOAD_DELAY = 0.01  # Delay before loading materialised table (seconds)
    DEFAULT_TASK_TIMEOUT = 30.0  # Timeout for waiting for task completion (seconds)
    DEFAULT_THREAD_POOL_SIZE = None  # None means auto-detect (use CPU count)

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.completed: set[str] = set()
        self.executing: set[str] = set()
        self.results: dict[str, Any] = {}
        self.materialised_tables: dict[str, ibis.Table] = {}
        self.display = get_display()  # Get global RichDisplay instance
        # Flow/task tracking
        self.flow: Flow | None = None
        self.flow_start_time: float | None = None

        # State database (optional)
        self.state_store: StateStore | None = None
        try:
            self.state_store = StateStore(config)
        except (ValueError, RuntimeError, OSError) as e:
            logger.warning(f"State database not available: {e}")

        # Phase 2: Initialize retry framework components
        # Retry manager and dead letter queue for handling transient failures
        from interlace.core.retry import CircuitBreaker, DeadLetterQueue, RetryManager

        # Get retry configuration from config
        retry_config = config.get("retry", {})

        # Create circuit breaker if configured
        circuit_breaker = None
        if retry_config.get("circuit_breaker", {}).get("enabled", False):
            cb_config = retry_config.get("circuit_breaker", {})
            circuit_breaker = CircuitBreaker(
                failure_threshold=cb_config.get("failure_threshold", 5),
                timeout=cb_config.get("timeout", 60.0),
                half_open_max_requests=cb_config.get("half_open_max_requests", 3),
                success_threshold=cb_config.get("success_threshold", 2),
            )
            logger.debug("Circuit breaker enabled for retry framework")

        # Create retry manager
        self.retry_manager = RetryManager(circuit_breaker=circuit_breaker)

        # Create dead letter queue
        self.dlq = DeadLetterQueue(state_store=self.state_store)
        logger.debug("Retry framework initialized")

        # Dependency loading locks to prevent race conditions
        self._dep_loading_locks: dict[str, asyncio.Lock] = {}

        # Schema caching for performance
        self._schema_cache: dict[str, ibis.Schema] = {}  # cache_key -> schema
        self._table_existence_cache: dict[tuple, bool] = {}  # (table_name, schema) -> exists
        self._dep_schema_cache: dict[str, str | None] = {}  # dep_name -> schema_name

        # Extract configuration values with defaults
        executor_config = config.get("executor", {})
        self.max_iterations = executor_config.get("max_iterations", self.DEFAULT_MAX_ITERATIONS)
        self.table_load_delay = executor_config.get("table_load_delay", self.DEFAULT_TABLE_LOAD_DELAY)
        self.task_timeout = executor_config.get("task_timeout", self.DEFAULT_TASK_TIMEOUT)

        # Cache configuration
        cache_config = executor_config.get("cache", {})
        self.max_schema_cache_size = cache_config.get("max_schema_cache_size", 1000)
        self.max_existence_cache_size = cache_config.get("max_existence_cache_size", 2000)
        self.schema_cache_ttl = cache_config.get("schema_cache_ttl")  # None = no expiration
        self.existence_cache_ttl = cache_config.get("existence_cache_ttl")  # None = no expiration

        # Create thread pool executor for Python model execution
        # Support "auto" or None for CPU-based sizing, or explicit integer value
        max_workers_config = executor_config.get("max_workers", self.DEFAULT_THREAD_POOL_SIZE)
        thread_pool_size = self._determine_thread_pool_size(max_workers_config)
        self.executor_pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        self.thread_pool_size = thread_pool_size  # Store for display visibility management

        # Initialize connections
        init_connections(self.config)

        # Store all connections for multi-connection support
        # Also store configs so we can create per-task connections (DuckDB is not thread-safe)
        self.connections: dict[str, Any] = {}  # Shared ibis backend connections
        self.connection_configs: dict[str, dict[str, Any]] = {}  # Connection configs for cloning

        # Determine default connection: check if "default" exists, otherwise use first connection or "duckdb_main"
        connections_config = self.config.get("connections", {})
        if "default" in connections_config:
            self.default_conn_name = "default"
        elif connections_config:
            # Use first connection name
            self.default_conn_name = next(iter(connections_config.keys()))
        else:
            self.default_conn_name = "duckdb_main"
        default_conn_name = self.default_conn_name  # Local alias

        # Load all connections from config (skip default, we'll load it separately below)
        connections_config = self.config.get("connections", {})
        for conn_name in connections_config.keys():
            # Skip default connection - we'll load it separately and handle errors there
            if conn_name == default_conn_name:
                continue
            try:
                conn_obj = get_connection(conn_name)
                self.connections[conn_name] = conn_obj.connection  # Store ibis backend
                self.connection_configs[conn_name] = conn_obj.config  # Store config for cloning
            except Exception as e:
                logger.warning(f"Could not load connection '{conn_name}': {e}")

        # Get default connection
        try:
            self.default_connection = get_connection(default_conn_name)
            self.con = self.default_connection.connection
            if default_conn_name not in self.connections:
                self.connections[default_conn_name] = self.con
                self.connection_configs[default_conn_name] = self.default_connection.config
        except (ValueError, RuntimeError, OSError) as e:
            # For connection errors, extract useful info from the error chain
            error_msg = str(e)
            conn_config = connections_config.get(default_conn_name, {})
            conn_path = conn_config.get("path", "")

            # Check if this is a lock error by examining the error message or underlying exception
            is_lock_error = "lock" in error_msg.lower() or "conflicting" in error_msg.lower()

            # Try to extract PID from the error chain
            pid = self._extract_pid_from_error_chain(e)

            if is_lock_error:
                # Build clean error message without nesting
                pid_info = f" (PID: {pid})" if pid != "unknown" else ""
                suggestions = [
                    "Close any DuckDB CLI sessions accessing this database",
                    "Close any other Python processes using this database",
                    "Close any database viewers or tools accessing this database",
                ]
                if pid != "unknown":
                    suggestions.append(f"Check if process {pid} is still running: `ps -p {pid}` or `kill {pid}`")
                suggestion_text = "\n  - ".join([""] + suggestions)

                raise ConnectionLockError(
                    f"Could not connect to database '{conn_path or default_conn_name}': File is locked{pid_info}.\n"
                    f"Suggested actions:{suggestion_text}",
                    pid=pid if pid != "unknown" else None,
                    path=conn_path or None,
                ) from None
            else:
                from interlace.exceptions import ConnectionError_ as InterlaceConnError

                raise InterlaceConnError(
                    f"Could not load connection '{default_conn_name}': {error_msg}\n"
                    f"Please check:\n"
                    f"  - Connection configuration in config.yaml\n"
                    f"  - Database file exists and is accessible: {conn_path or 'N/A'}\n"
                    f"  - File permissions are correct\n"
                    f"  - No other processes are using the database"
                ) from e

        # Initialize execution components (after connections are set up)
        self.data_converter = DataConverter()
        self.change_detector = ChangeDetector(state_store=self.state_store)
        self.connection_manager = TaskConnectionManager(
            connection_configs=self.connection_configs,
            connections=self.connections,
            default_conn_name=self.default_conn_name,
            state_store=self.state_store,
        )
        # Set default connection in TaskConnectionManager after it's initialized
        if hasattr(self, "con"):
            self.connection_manager._default_connection = self.con
        self.schema_manager = SchemaManager(
            data_converter=self.data_converter,
            schema_cache=self._schema_cache,
            table_existence_cache=self._table_existence_cache,
            max_schema_cache_size=self.max_schema_cache_size,
            max_existence_cache_size=self.max_existence_cache_size,
            schema_cache_ttl=self.schema_cache_ttl,
            existence_cache_ttl=self.existence_cache_ttl,
        )
        self.dependency_loader = DependencyLoader(
            materialised_tables=self.materialised_tables,
            schema_manager=self.schema_manager,
            dep_loading_locks=self._dep_loading_locks,
            dep_schema_cache=self._dep_schema_cache,
        )

        # Initialize materializers
        self.materializers = {
            "table": TableMaterializer(),
            "view": ViewMaterializer(),
            "ephemeral": EphemeralMaterializer(),
        }

        # Initialize strategies
        self.strategies = {
            "merge_by_key": MergeByKeyStrategy(),
            "append": AppendStrategy(),
            "replace": ReplaceStrategy(),
            "none": NoneStrategy(),
        }

        # Initialize MaterializationManager after strategies are set up
        self.materialization_manager = MaterializationManager(
            schema_manager=self.schema_manager,
            data_converter=self.data_converter,
            strategies=self.strategies,
            flow=None,  # Will be set when flow is created
            get_row_count_func=self._get_row_count,
        )

        # Initialize ModelExecutor after all components are set up
        # Use configuration object to reduce coupling
        # Note: flow and display will be set later when flow is created
        # Backfill overrides (injected via config dict by execute_models / programmatic API)
        self._since = config.get("_since")
        self._until = config.get("_until")

        model_executor_config = ModelExecutorConfig(
            change_detector=self.change_detector,
            connection_manager=self.connection_manager,
            dependency_loader=self.dependency_loader,
            schema_manager=self.schema_manager,
            materialisation_manager=self.materialization_manager,
            data_converter=self.data_converter,
            materialisers=self.materializers,
            materialised_tables=self.materialised_tables,
            executor_pool=self.executor_pool,
            state_store=self.state_store,
            flow=None,  # Will be set when flow is created
            display=self.display,
            # Phase 2: Retry framework
            retry_manager=self.retry_manager,
            dlq=self.dlq,
            # Backfill overrides
            since=self._since,
            until=self._until,
            get_row_count_func=self._get_row_count,
            update_model_last_run_func=self.change_detector.update_model_metadata,
            log_model_end_func=self._log_model_end,
            prepare_model_execution_func=self._prepare_model_execution,
            store_materialised_table_func=self._store_materialised_table,
        )
        self.model_executor = ModelExecutor(model_executor_config)

        # Initialize ExecutionOrchestrator after all components are set up
        # Pass a callback to update Executor's flow reference when flow is created
        def set_executor_flow(flow):
            self.flow = flow

        self.execution_orchestrator = ExecutionOrchestrator(
            model_executor=self.model_executor,
            change_detector=self.change_detector,
            materialization_manager=self.materialization_manager,
            state_store=self.state_store,
            display=self.display,
            config=self.config,
            max_iterations=self.max_iterations,
            task_timeout=self.task_timeout,
            thread_pool_size=self.thread_pool_size,
            executor_flow_setter=set_executor_flow,
        )

    def _determine_thread_pool_size(self, max_workers: int | str | None) -> int:
        """
        Determine thread pool size from configuration.

        For I/O-bound workloads (API calls, database queries, file operations),
        using more threads than CPUs is optimal since threads spend time waiting.
        Default uses: min(32, (CPU count * 2) + 4) which provides good balance.

        Args:
            max_workers: Configuration value - can be:
                - None or "auto": Use optimized formula: min(32, (CPU count * 2) + 4)
                - Integer: Use explicit value
                - String "auto": Use optimized formula

        Returns:
            Integer thread pool size
        """
        if max_workers is None or (isinstance(max_workers, str) and max_workers.lower() == "auto"):
            # Auto-detect: use optimized formula for I/O-bound workloads
            # Formula: min(32, (CPU count * 2) + 4)
            # - 2x multiplier accounts for I/O wait time
            # - +4 adds buffer for concurrent operations
            # - Cap at 32 to prevent excessive thread creation
            cpu_count = os.cpu_count()
            if cpu_count is None:
                logger.warning("Could not determine CPU count, defaulting to 8 workers")
                return 8

            # Optimized for I/O-bound: 2x CPUs + 4, capped at 32
            optimal_size = min(32, (cpu_count * 2) + 4)
            return optimal_size
        elif isinstance(max_workers, int):
            if max_workers <= 0:
                raise ValueError(f"max_workers must be positive, got {max_workers}")
            return max_workers
        else:
            raise ValueError(
                f"max_workers must be an integer, 'auto', or None, got {type(max_workers).__name__}: {max_workers}"
            )

    def _extract_pid_from_error(self, error_msg: str) -> str:
        """Extract process ID from error message if present."""
        import re

        # Look for "PID 12345" pattern
        match = re.search(r"PID\s+(\d+)", error_msg)
        if match:
            return match.group(1)
        return "unknown"

    def _extract_pid_from_error_chain(self, exception: Exception) -> str:
        """Extract process ID from exception chain by traversing __cause__ and __context__."""
        import re

        current = exception

        # Check current exception
        error_str = str(current)
        match = re.search(r"PID\s+(\d+)", error_str)
        if match:
            return match.group(1)

        # Traverse __cause__ chain
        cause = getattr(current, "__cause__", None)
        while cause:
            error_str = str(cause)
            match = re.search(r"PID\s+(\d+)", error_str)
            if match:
                return match.group(1)
            cause = getattr(cause, "__cause__", None)

        # Traverse __context__ chain (for chained exceptions)
        context = getattr(current, "__context__", None)
        while context:
            error_str = str(context)
            match = re.search(r"PID\s+(\d+)", error_str)
            if match:
                return match.group(1)
            context = getattr(context, "__context__", None)

        return "unknown"

    def _log_model_start(self, model_name: str, model_info: dict[str, Any]) -> None:
        """
        Log model execution start with smart formatting.

        Args:
            model_name: Name of the model
            model_info: Model configuration dictionary
        """
        model_type = model_info.get("type", "python")
        materialise = model_info.get("materialise", "table")
        schema = model_info.get("schema", "public")
        strategy_name = model_info.get("strategy")

        # Build log message with context
        parts = [f"Starting {model_type} model '{model_name}'"]
        if schema != "public":
            parts.append(f"schema={schema}")
        parts.append(f"materialize={materialise}")
        if strategy_name:
            parts.append(f"strategy={strategy_name}")

        logger.info(" | ".join(parts))

    def _log_model_end(
        self,
        model_name: str,
        model_info: dict[str, Any],
        success: bool,
        duration: float,
        rows_processed: int | None = None,
        rows_ingested: int | None = None,
        rows_inserted: int | None = None,
        rows_updated: int | None = None,
        rows_deleted: int | None = None,
        schema_changes: int = 0,
        error: str | None = None,
    ) -> None:
        """
        Log model execution end with timing and statistics.

        Args:
            model_name: Name of the model
            model_info: Model configuration dictionary
            success: Whether execution succeeded
            duration: Execution duration in seconds
            rows_processed: Final row count after materialization
            rows_ingested: Input row count before strategy
            rows_inserted: Number of rows inserted
            rows_updated: Number of rows updated
            rows_deleted: Number of rows deleted
            schema_changes: Number of schema changes applied
            error: Error message if failed
        """
        # Format duration (show milliseconds for fast operations)
        if duration < 1.0:
            duration_str = f"{duration*1000:.0f}ms"
        elif duration < 60.0:
            duration_str = f"{duration:.2f}s"
        else:
            minutes = int(duration // 60)
            seconds = duration % 60
            duration_str = f"{minutes}m{seconds:.1f}s"

        if success:
            # Build success message with statistics
            parts = [f"Completed '{model_name}' in {duration_str}"]

            # Add row statistics if available
            stats = []
            if rows_processed is not None:
                stats.append(f"{rows_processed:,} rows")
            if rows_inserted is not None and rows_inserted > 0:
                stats.append(f"{rows_inserted:,} inserted")
            if rows_updated is not None and rows_updated > 0:
                stats.append(f"{rows_updated:,} updated")
            if rows_deleted is not None and rows_deleted > 0:
                stats.append(f"{rows_deleted:,} deleted")
            if schema_changes > 0:
                stats.append(f"{schema_changes} schema change(s)")

            if stats:
                parts.append("(" + ", ".join(stats) + ")")

            logger.info(" | ".join(parts))
        else:
            # Build error message with timing
            error_msg = f"Failed '{model_name}' after {duration_str}"
            if error:
                error_msg += f": {error}"
            logger.error(error_msg)

    def _prepare_model_execution(
        self, model_name: str, model_info: dict[str, Any]
    ) -> tuple[str, str, str, str | None, float]:
        """
        Prepare model execution by extracting configuration and initializing tracking.

        Returns:
            Tuple of (model_type, materialise, schema, strategy_name, start_time)
        """

        start_time = time.time()

        # Track timing for debugging parallelization
        if not hasattr(self, "model_timing"):
            self.model_timing = {}
        self.model_timing[model_name] = {"start_time": start_time, "end_time": None}

        # Extract model configuration
        model_type = model_info.get("type", "python")
        materialise = model_info.get("materialise", "table")
        schema = model_info.get("schema", "public")
        strategy_name = model_info.get("strategy")

        # Log model start
        self._log_model_start(model_name, model_info)

        # Update task status to running
        if model_name in self.flow.tasks:
            task = self.flow.tasks[model_name]
            task.start()
            self.display.update_from_flow()

        return model_type, materialise, schema, strategy_name, start_time

    async def _store_materialised_table(
        self,
        model_name: str,
        result: ibis.Table,
        materialise: str,
        schema: str,
        model_conn: ibis.BaseBackend,
    ) -> None:
        """
        Store materialised table for downstream models to use.
        """
        if materialise in ("table", "view"):
            # Always store the result table first to ensure it's available
            # Then try to load from database if possible (for better performance)
            self.materialised_tables[model_name] = result

            try:
                # Small delay to ensure table is visible after materialization
                await asyncio.sleep(self.table_load_delay)
                # Use utility function to reduce duplication
                loaded_table = try_load_table(model_conn, model_name, [schema, "main", "public", None])
                if loaded_table is not None:
                    # Only update if we successfully loaded - otherwise keep the result table
                    self.materialised_tables[model_name] = loaded_table
                # If all schema attempts failed, that's fine - we already have result stored
            except Exception:
                # Any exception here is non-fatal - we already stored the result table
                # Don't log or raise - just continue with the result table we have
                pass
        elif materialise == "ephemeral":
            self.materialised_tables[model_name] = result

    def _get_row_count(
        self,
        result: Any,
        model_name: str,
        materialise: str,
        schema: str,
        model_conn: ibis.BaseBackend,
    ) -> int | None:
        """
        Get row count from materialised table or result.

        Returns:
            Row count or None if unable to determine
        """
        try:
            # First, try to use the stored materialised table if available
            # This avoids reloading from database which may fail due to timing/schema issues
            if model_name in self.materialised_tables:
                stored_table = self.materialised_tables[model_name]
                if isinstance(stored_table, ibis.Table):
                    count = get_row_count_efficient(model_conn, stored_table)
                    if count is not None:
                        return count

            # For table/view materialization, try to use result directly first
            # This is more reliable than trying to load from database immediately after creation
            if isinstance(result, ibis.Table):
                count = get_row_count_efficient(model_conn, result)
                if count is not None:
                    return count

            if materialise in ("table", "view"):
                # Try to load from database as fallback
                # But don't fail if this doesn't work - we already tried the result
                count = get_row_count_efficient(model_conn, model_name, schema=schema)
                if count is not None:
                    return count

                # Try other schemas
                for try_schema in ["main", "public", None]:
                    count = get_row_count_efficient(model_conn, model_name, schema=try_schema)
                    if count is not None:
                        return count
            elif hasattr(result, "__len__"):
                try:
                    return len(result)
                except TypeError as e:
                    logger.debug(f"Could not get length from result for {model_name}: {e}")
        except Exception as count_error:
            logger.debug(f"Could not determine row count for {model_name}: {count_error}")
        # Return None if we can't determine row count - don't fail the task
        return None

    async def _load_dependency_with_lock(
        self,
        dep_name: str,
        model_conn: ibis.BaseBackend,
        models: dict[str, dict[str, Any]],
        schema: str,
    ) -> ibis.Table | None:
        """
        Load a dependency table with lock to optimize performance.

        Note: Locks are NOT required for correctness - multiple tasks can safely load
        the same dependency in parallel (they're just reading table references). However,
        the lock prevents redundant work when many tasks need the same dependency:
        - Without lock: 10 tasks might all try multiple schemas, causing 10x redundant queries
        - With lock: Only one task does the lookup, others reuse the cached result

        This is a performance optimization, not a correctness requirement.

        Args:
            dep_name: Name of the dependency to load
            model_conn: Connection to use for loading
            models: All models dictionary
            schema: Current model's schema

        Returns:
            ibis.Table if dependency was loaded successfully, None otherwise
        """
        # Create lock for this dependency if it doesn't exist
        if dep_name not in self._dep_loading_locks:
            self._dep_loading_locks[dep_name] = asyncio.Lock()

        async with self._dep_loading_locks[dep_name]:
            # Check again after acquiring lock (another task may have loaded it)
            if dep_name in self.materialised_tables:
                return self.materialised_tables[dep_name]

            # Try to load from connections
            # Check if dependency model specifies a connection
            dep_model_info = models.get(dep_name, {})
            dep_conn = model_conn  # Use same task connection
            dep_schema = dep_model_info.get("schema", "public")

            # Try cached schema location first
            try_schemas = [self._dep_schema_cache.get(dep_name)]
            # Remove None if present and add other schemas
            try_schemas = [s for s in try_schemas if s is not None]
            try_schemas.extend([dep_schema, "main", schema, "public", None])
            # Remove duplicates while preserving order
            seen = set()
            try_schemas = [s for s in try_schemas if not (s in seen or seen.add(s))]

            # Try to load table from dependency's connection and schema
            last_error = None
            for try_schema in try_schemas:
                try:
                    if try_schema:
                        dep_table = dep_conn.table(dep_name, database=try_schema)
                    else:
                        dep_table = dep_conn.table(dep_name)

                    # Cache successful schema location
                    self._dep_schema_cache[dep_name] = try_schema
                    # Store in materialised_tables for future use
                    self.materialised_tables[dep_name] = dep_table
                    return dep_table
                except Exception as schema_error:
                    logger.debug(f"Could not load {dep_name} from schema {try_schema}: {schema_error}")
                    last_error = schema_error
                    continue

            # Failed to load from any schema - log warning with context
            logger.warning(
                f"Failed to load dependency '{dep_name}' from any schema. "
                f"Tried: {try_schemas}. Last error: {last_error}. "
                f"Downstream models may fail if this dependency is required."
            )
            return None

    async def _execute_python_model(
        self,
        model_info: dict[str, Any],
        dependency_tables: dict[str, ibis.Table],
        connection: ibis.BaseBackend,
    ) -> ibis.Table | None:
        """Execute Python model function (supports both async and sync functions)."""
        func = model_info.get("function")
        if not func:
            raise ValueError("Python model missing function")

        # Contextvars don't propagate to executor threads, so we set context in the thread
        try:
            current_conn = get_context_connection()
        except RuntimeError:
            current_conn = connection

        func_kwargs = dependency_tables.copy()

        # Check if function is async or sync
        is_async = inspect.iscoroutinefunction(func)

        if is_async:
            # For async functions, await directly (no executor needed)
            set_connection(current_conn)
            result = await func(**func_kwargs)
        else:
            # For sync functions, run in executor to avoid blocking event loop
            loop = asyncio.get_event_loop()

            def execute_func():
                set_connection(current_conn)
                return func(**func_kwargs)

            result = await loop.run_in_executor(self.executor_pool, execute_func)

        if result is None:
            return None

        if hasattr(result, "__iter__") and not isinstance(result, (str, bytes, ibis.Table)):
            try:
                import types

                if isinstance(result, types.GeneratorType):
                    result = list(result)
            except (TypeError, AttributeError):
                pass

        if isinstance(result, ibis.Table):
            return result
        else:
            # Get fields from model_info if provided
            fields = model_info.get("fields")
            return self.data_converter.convert_to_ibis_table(result, fields=fields)

    async def _setup_reference_table(
        self,
        data: Any,
        model_name: str,
        connection: ibis.BaseBackend,
        model_info: dict[str, Any] | None = None,
    ) -> ibis.Table:
        """
        Setup reference table for schema validation.

        For dict/list/DataFrame: Converts to ibis.Table via ibis.memtable(), then creates temporary reference table.
        This allows schema validation before materialization.

        Args:
            data: Python data structure (dict/list/DataFrame)
            model_name: Model name
            connection: ibis connection backend
            model_info: Optional model info dict to get fields

        Returns:
            ibis.Table: Reference table (temporary table)
        """
        fields = model_info.get("fields") if model_info else None
        table_expr = self.data_converter.convert_to_ibis_table(data, fields=fields)
        ref_table_name = f"_interlace_tmp_{model_name}"

        try:
            df = table_expr.execute()
            connection.create_table(ref_table_name, obj=df, temp=True)
            return connection.table(ref_table_name)
        except Exception as e:
            # Fallback: if temp table creation fails, use memtable directly
            logger.warning(f"Could not create reference table {ref_table_name}: {e}, using memtable directly")
            return table_expr

    async def _validate_and_update_schema(
        self,
        table_ref: ibis.Table,
        model_name: str,
        schema: str,
        connection: ibis.BaseBackend,
        model_info: dict[str, Any] | None = None,
    ) -> int:
        """
        Validate schema compatibility and handle schema evolution.

        Uses the new schema validation module to compare schemas and apply changes.
        Respects fields parameter (adds/replaces fields in schema if specified).
        Safe column removal: preserves columns, new data has NULL for missing columns.

        Args:
            table_ref: ibis.Table reference (new data)
            model_name: Model name
            schema: Schema/database name
            connection: ibis connection backend
            model_info: Optional model info dict to get fields

        Returns:
            Number of schema changes applied
        """
        from interlace.schema.evolution import apply_schema_changes, should_apply_schema_changes
        from interlace.schema.tracking import track_schema_version
        from interlace.schema.validation import validate_schema

        # Check if target table exists using cached check
        table_exists = self.schema_manager.check_table_exists(connection, model_name, schema)

        new_schema = table_ref.schema()
        fields = model_info.get("fields") if model_info else None
        # Convert fields to ibis schema for validation
        from interlace.utils.schema_utils import fields_to_ibis_schema

        fields_schema = fields_to_ibis_schema(fields)

        # Check cache first (use 'main' as default schema for consistency)
        cache_key = f"{schema or 'main'}.{model_name}"
        if cache_key in self._schema_cache:
            cached_schema = self._schema_cache[cache_key]
            # Compare schemas - if identical, no changes needed
            if cached_schema == new_schema:
                return 0

        # Get existing schema if table exists
        existing_schema = None
        if table_exists:
            try:
                existing_table = connection.table(model_name, database=schema)
                existing_schema = existing_table.schema()
            except (FileNotFoundError, KeyError) as e:
                # Table doesn't exist - no schema validation needed
                logger.debug(f"Table {schema}.{model_name} not found for schema validation: {e}")
                existing_schema = None
            except (PermissionError, RuntimeError) as e:
                # Permission or access issue - log warning but continue
                logger.warning(f"Could not access table {schema}.{model_name} for schema validation: {e}")
                existing_schema = None
            except Exception as e:
                # Unexpected error - log for debugging
                logger.debug(f"Unexpected error loading schema for {schema}.{model_name}: {e}")
                existing_schema = None

        # Validate schema
        validation_result = validate_schema(existing_schema, new_schema, fields_schema)

        if validation_result.errors:
            # Log errors but don't fail (non-fatal)
            logger.warning(f"Schema validation errors for {model_name}: {', '.join(validation_result.errors)}")
            return 0

        # Track schema version (informative only)
        # Track initial schema if table doesn't exist, or track changes if schema changed
        should_track = False
        if existing_schema is None:
            # First time creating table - track initial schema
            should_track = True
        elif should_apply_schema_changes(existing_schema, new_schema, fields_schema):
            # Schema changed - track new version
            should_track = True

        if should_track:
            try:
                primary_key = model_info.get("primary_key") if model_info else None
                track_schema_version(connection, model_name, schema, new_schema, primary_key)
            except Exception as e:
                logger.debug(f"Could not track schema version for {model_name}: {e}")

        # Apply schema changes if needed
        if existing_schema is not None and should_apply_schema_changes(existing_schema, new_schema, fields_schema):
            try:
                changes_count = apply_schema_changes(
                    connection, model_name, schema, existing_schema, new_schema, fields_schema
                )
                # Update cache with new schema
                self._schema_cache[cache_key] = new_schema
                return changes_count
            except Exception as e:
                logger.warning(f"Failed to apply schema changes for {model_name}: {e}")
                return 0

        # Update cache even if no changes
        if existing_schema is None:
            self._schema_cache[cache_key] = new_schema

        return 0

    async def _execute_sql_model(
        self,
        model_info: dict[str, Any],
        dependency_tables: dict[str, ibis.Table],
        connection: ibis.BaseBackend,
    ) -> ibis.Table:
        """
        Execute SQL model query.

        Executes a SQL query and returns the result as an ibis.Table. Dependency tables
        are registered as temporary tables so they can be referenced in the SQL query.

        Args:
            model_info: Model configuration dictionary containing the SQL query
            dependency_tables: Dictionary mapping dependency names to ibis.Table objects
            connection: Database connection backend

        Returns:
            ibis.Table representing the query result

        Raises:
            ValueError: If SQL query is missing
        """
        query = model_info.get("query")
        if not query:
            raise ValueError("SQL model missing query")

        # Handle cross-connection references in SQL queries
        # Pattern: "connectionname.database.tablename" or "connectionname.tablename"
        # For now, if DuckDB is primary connection, use ATTACH (already configured in DuckDBConnection)
        # For other backends, cross-connection queries need to be resolved differently

        # Register dependency tables as temporary tables so SQL can reference by unqualified name
        for dep_name, dep_table in dependency_tables.items():
            try:
                if not check_table_exists(connection, dep_name, None):
                    df = dep_table.execute()
                    connection.create_table(dep_name, obj=df, temp=True)
            except Exception:
                pass

        # Workaround for ibis issue with CTEs referenced in subqueries
        # When a CTE is referenced both in the main query and in a subquery, ibis may duplicate
        # the CTE definition during compilation, causing a "Duplicate CTE name" error.
        # We detect this by trying to execute the query, and if it fails, use a workaround.
        model_name = model_info.get("name", "temp_query")
        temp_table_name = f"_interlace_cte_workaround_{model_name}"

        def execute_with_workaround():
            """Execute query using underlying DuckDB connection as workaround."""
            if hasattr(connection, "con"):
                duckdb_con = connection.con
                if hasattr(duckdb_con, "execute") and hasattr(duckdb_con, "fetchdf"):
                    cursor = duckdb_con.execute(query)
                    df = cursor.fetchdf()
                    connection.create_table(temp_table_name, obj=df, temp=True)
                    return connection.table(temp_table_name)
            return None

        try:
            # Log SQL model execution
            logger.debug(f"Executing SQL model '{model_name}'")

            # Try the normal path first
            result = connection.sql(query)

            # Test execution early to catch CTE issues
            try:
                result.execute()
                return connection.sql(query)
            except Exception as exec_error:
                error_msg = str(exec_error).lower()
                # Check if it's a duplicate CTE error
                if "duplicate cte" in error_msg or ("duplicate" in error_msg and "cte" in error_msg):
                    # Use workaround: execute directly and create temp table
                    workaround_result = execute_with_workaround()
                    if workaround_result is not None:
                        return workaround_result
                    # If workaround failed, provide helpful error
                    raise ValueError(
                        f"Query failed with duplicate CTE error. This appears to be an ibis issue "
                        f"with CTEs referenced in subqueries. The query structure is valid SQL, "
                        f"but ibis duplicates the CTE during compilation. "
                        f"Original error: {exec_error}"
                    ) from exec_error
                # Re-raise if it's not a CTE error
                raise
        except Exception as e:
            error_msg = str(e).lower()
            # Check if error happened during connection.sql() call (some ibis versions)
            if "duplicate cte" in error_msg or ("duplicate" in error_msg and "cte" in error_msg):
                # Use workaround
                workaround_result = execute_with_workaround()
                if workaround_result is not None:
                    return workaround_result
                raise ValueError(
                    f"Query failed with duplicate CTE error and workaround unavailable. "
                    f"This may be an ibis issue with CTEs referenced in subqueries. "
                    f"Original error: {e}"
                ) from e
            # Re-raise if it's not a CTE error
            raise

    async def execute_dynamic(
        self, models: dict[str, dict[str, Any]], graph: DependencyGraph, force: bool = False
    ) -> dict[str, Any]:
        """
        Execute models dynamically as dependencies complete.

        Phase 0: Dynamic parallel execution - execute models as soon as dependencies are ready.

        Args:
            models: Dictionary of model definitions
            graph: Dependency graph
            force: Force execution (bypass change detection)
        """
        # Delegate to ExecutionOrchestrator
        results = await self.execution_orchestrator.execute_dynamic(models, graph, force)
        # Update flow reference for backward compatibility (needed for _prepare_model_execution)
        self.flow = self.execution_orchestrator.flow
        # Also update flow on model_executor and materialization_manager for consistency
        if self.flow:
            self.model_executor.flow = self.flow
            self.materialization_manager.flow = self.flow
        return results

    async def _execute_loop(
        self,
        models: dict[str, dict[str, Any]],
        graph: DependencyGraph,
        pending: set[str],
        executing: set[str],
        completed: set[str],
        succeeded: set[str],
        results: dict[str, Any],
        task_map: dict[str, asyncio.Task],
        ready: set[str],
    ) -> dict[str, Any]:
        """
        Internal execution loop (extracted for progress display context).

        Manages the dynamic parallel execution of models, starting models as soon as
        their dependencies are satisfied and processing completions to unlock dependents.

        Args:
            models: Dictionary of all model definitions
            graph: Dependency graph
            pending: Set of pending model names
            executing: Set of currently executing model names
            completed: Set of completed model names
            results: Dictionary to store execution results
            task_map: Dictionary mapping model names to asyncio.Task objects
            ready: Set of model names ready to execute

        Returns:
            Dictionary mapping model names to execution results
        """
        new_ready = set()
        iteration = 0
        while pending or executing:
            iteration += 1
            if iteration > self.max_iterations:  # Safety check to prevent infinite loops
                logger.error(
                    f"Loop iteration limit reached. "
                    f"Pending: {pending}, Executing: {executing}, "
                    f"Ready: {ready}, Completed: {completed}"
                )
                break

            # Start executing ready models
            new_ready = set()
            for model_name in list(ready):
                if model_name not in executing and model_name in pending:
                    pending.remove(model_name)
                    executing.add(model_name)
                    ready.remove(model_name)

                    # Start model execution
                    await self._start_model_execution(model_name, models, graph, task_map)

            # If no tasks are executing and nothing is pending, we're done
            if not executing and not pending:
                break

            # Deadlock detection: if there are pending models but none are ready and nothing is executing
            # This means there's a circular dependency or missing dependency
            if not executing and pending and not ready:
                # Check for circular dependencies or missing dependencies
                missing_deps = {}
                for model_name in pending:
                    deps = graph.get_dependencies(model_name)
                    missing = [d for d in deps if d not in completed and d not in models]
                    if missing:
                        missing_deps[model_name] = missing

                if missing_deps:
                    logger.error(f"Deadlock detected - models have missing dependencies: {missing_deps}")
                else:
                    logger.error(
                        f"Deadlock detected - circular dependencies or unstartable models in pending: {pending}"
                    )
                break

            # Wait for at least one task to complete
            if executing:
                await self._wait_for_task_completion(
                    executing,
                    task_map,
                    results,
                    models,
                    graph,
                    pending,
                    ready,
                    new_ready,
                    completed,
                    succeeded,
                )

            # Update ready list: add newly ready models from pending
            # Only mark as ready if ALL dependencies succeeded (not just completed)
            # Also check for models that should be skipped (have failed dependencies)
            for model_name in list(pending):
                deps = graph.get_dependencies(model_name)

                # Check if any dependency failed - if so, mark as skipped
                failed_deps = [d for d in deps if d in completed and d not in succeeded]
                if failed_deps:
                    # At least one dependency failed - mark as skipped
                    pending.remove(model_name)
                    completed.add(model_name)
                    # Don't add to succeeded

                    # Create error message
                    error_msg = f"Dependencies {failed_deps} failed"
                    results[model_name] = {
                        "status": "error",
                        "model": model_name,
                        "error": error_msg,
                        "failed_dependencies": failed_deps,
                    }

                    # Update task status
                    if model_name in self.flow.tasks:
                        task = self.flow.tasks[model_name]
                        task.skip()
                        task.error_message = error_msg
                        if self.state_store:
                            self.state_store.save_task(task)

                    # Log and update display
                    logger.error(f"Model '{model_name}' skipped: {error_msg}")
                    if self.display.enabled:
                        self.display.add_error(model_name, f"Skipped due to failed dependencies: {failed_deps}")
                    self.display.update_from_flow()
                elif all(dep in succeeded for dep in deps):
                    # All dependencies succeeded - mark as ready
                    new_ready.add(model_name)
                    # Update task status for newly ready models
                    if model_name in self.flow.tasks:
                        task = self.flow.tasks[model_name]
                        task.mark_ready()
                        if self.state_store:
                            self.state_store.save_task(task)

                    # Update display for newly ready models
                    self.display.update_from_flow()

                    # Note: Model start logging happens in _prepare_model_execution()
                    # which is called at the beginning of execute_model()

            # Add newly ready models to ready set
            ready.update(new_ready)

        return results

    async def _start_model_execution(
        self,
        model_name: str,
        models: dict[str, dict[str, Any]],
        graph: DependencyGraph,
        task_map: dict[str, asyncio.Task],
    ):
        """
        Start execution of a model.

        Args:
            model_name: Name of the model to start
            models: Dictionary of all model definitions
            graph: Dependency graph
            task_map: Dictionary mapping model names to asyncio.Task objects
        """
        # Update task to show model is ready to execute
        if model_name in self.flow.tasks:
            task = self.flow.tasks[model_name]
            task.mark_ready()  # Dependencies satisfied
            # Note: task.start() and logging happen in execute_model() via _prepare_model_execution()

        # Progress will be updated in execute_model() when execution actually starts

        # Create task for model execution
        # Get force flag from executor instance (passed to execute_dynamic)
        force = getattr(self, "_force_execution", False)
        task = asyncio.create_task(
            self.model_executor.execute_model(model_name, models[model_name], models, force=force)
        )
        task_map[model_name] = task

    async def _wait_for_task_completion(
        self,
        executing: set[str],
        task_map: dict[str, asyncio.Task],
        results: dict[str, Any],
        models: dict[str, dict[str, Any]],
        graph: DependencyGraph,
        pending: set[str],
        ready: set[str],
        new_ready: set[str],
        completed: set[str],
        succeeded: set[str],
    ):
        """
        Wait for at least one task to complete and process the result.

        Args:
            executing: Set of currently executing model names
            task_map: Dictionary mapping model names to asyncio.Task objects
            results: Dictionary to store execution results
            models: Dictionary of all model definitions
            graph: Dependency graph
            pending: Set of pending model names
            new_ready: Set to add newly ready models to
            completed: Set of completed model names
        """
        executing_tasks = [task_map[m] for m in executing if m in task_map]
        if not executing_tasks:
            logger.warning(f"No executing tasks found but executing set is: {executing}")
            return

        try:
            done, pending_tasks = await asyncio.wait(
                executing_tasks,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=self.task_timeout,  # Add timeout to detect hangs
            )
        except (KeyboardInterrupt, asyncio.CancelledError) as e:
            # If interrupted, cancel all tasks and re-raise
            for task in executing_tasks:
                if not task.done():
                    task.cancel()
            # Re-raise to be handled by outer handler
            raise KeyboardInterrupt("Execution interrupted") from e
        except TimeoutError:
            logger.error(f"Timeout waiting for tasks to complete. Executing: {executing}")
            # Cancel hanging tasks
            for task in executing_tasks:
                task.cancel()
            return

        # Process completed tasks
        for task in done:
            try:
                result = await task
                await self._process_completed_task(
                    task,
                    result,
                    executing,
                    completed,
                    succeeded,
                    results,
                    task_map,
                    models,
                    graph,
                    pending,
                    new_ready,
                )
            except Exception as e:
                # Capture exception info while still in exception context
                import sys

                exc_info = sys.exc_info()
                await self._process_failed_task(
                    task,
                    e,
                    executing,
                    completed,
                    succeeded,
                    results,
                    task_map,
                    models,
                    graph,
                    pending,
                    ready,
                    new_ready,
                    exc_info=exc_info,
                )

    async def _process_completed_task(
        self,
        task: asyncio.Task,
        result: dict[str, Any],
        executing: set[str],
        completed: set[str],
        succeeded: set[str],
        results: dict[str, Any],
        task_map: dict[str, asyncio.Task],
        models: dict[str, dict[str, Any]],
        graph: DependencyGraph,
        pending: set[str],
        new_ready: set[str],
    ):
        """
        Process a successfully completed task.

        Args:
            task: The completed asyncio.Task
            result: Execution result dictionary
            executing: Set of currently executing model names
            completed: Set of completed model names
            results: Dictionary to store execution results
            task_map: Dictionary mapping model names to asyncio.Task objects
            models: Dictionary of all model definitions
            graph: Dependency graph
            pending: Set of pending model names
            new_ready: Set to add newly ready models to
        """
        # Find which model completed
        for model_name in list(executing):
            if model_name in task_map and task_map[model_name] == task:
                executing.remove(model_name)
                completed.add(model_name)
                results[model_name] = result
                if model_name in task_map:
                    del task_map[model_name]

                # Check if result indicates success or error
                result_status = result.get("status", "success")

                # Update task status
                if model_name in self.flow.tasks:
                    flow_task = self.flow.tasks[model_name]
                    if result_status == "error":
                        flow_task.complete(success=False)
                        error_msg = result.get("error", "Unknown error")
                        flow_task.error_message = error_msg

                        # Log error - models can return errors as results instead of raising exceptions
                        error_message = f"Model '{model_name}' failed: {error_msg}"
                        error_traceback = result.get("traceback") or result.get("error_stacktrace")

                        # Log error with traceback if available
                        # NOTE: Do this BEFORE update_from_flow() to avoid potential deadlocks
                        # The logging handler might access display state, and update_from_flow()
                        # uses a lock, so doing them in sequence avoids deadlocks
                        try:
                            # Get exc_info from result if available (for Rich formatting)
                            exc_info = result.get("exc_info")
                            if exc_info and len(exc_info) == 3:
                                # Use exc_info for RichHandler to format traceback nicely
                                # This will show the styled traceback in console
                                logger.error(
                                    error_message,
                                    exc_info=exc_info,
                                    extra={"model_name": model_name},
                                )
                            elif error_traceback:
                                # Fallback: include traceback in message if exc_info not available
                                logger.error(
                                    f"{error_message}\n{error_traceback}",
                                    extra={"model_name": model_name},
                                )
                            else:
                                # No traceback available, just log the error message
                                logger.error(error_message, exc_info=True, extra={"model_name": model_name})

                            # Also add error to display for error panel (include traceback if available)
                            if self.display.enabled:
                                if error_traceback:
                                    self.display.add_error(model_name, f"{error_msg}\n{error_traceback}")
                                else:
                                    self.display.add_error(model_name, error_msg)
                        except Exception as log_error:
                            # If logging fails, at least try to log a simple error message
                            try:
                                logger.error(f"Model '{model_name}' failed: {error_msg} (logging error: {log_error})")
                            except Exception:
                                # Last resort - print to stderr
                                import sys

                                # Fallback: use stderr if logging completely fails

                                sys.stderr.write(f"ERROR: Model '{model_name}' failed: {error_msg}\n")
                                sys.stderr.write(f"ERROR: Logging also failed: {log_error}\n")
                        # Don't add to succeeded - model failed
                    else:
                        flow_task.complete(success=True)
                        flow_task.rows_processed = result.get("rows")
                        # schema_changes already set during execution
                        # Add to succeeded set - model completed successfully
                        succeeded.add(model_name)

                    # Save task to state database
                    if self.state_store:
                        self.state_store.save_task(flow_task)

                # Update display after task completion
                # NOTE: This is called AFTER logging to avoid potential deadlocks
                # The logging handler might access display state, so we do logging first
                # then update display separately
                self.display.update_from_flow()

                # Check dependents that are now ready
                # Only mark as ready if ALL dependencies succeeded (not just completed)
                dependents = graph.get_dependents(model_name)
                for dependent in dependents:
                    deps = graph.get_dependencies(dependent)
                    if dependent in pending and all(d in succeeded for d in deps):
                        new_ready.add(dependent)
                        # Update task status for newly ready models
                        if dependent in self.flow.tasks:
                            dep_task = self.flow.tasks[dependent]
                            dep_task.mark_ready()
                            if self.state_store:
                                self.state_store.save_task(dep_task)
                        # Update display
                        self.display.update_from_flow()
                break

    async def _process_failed_task(
        self,
        task: asyncio.Task,
        error: Exception,
        executing: set[str],
        completed: set[str],
        succeeded: set[str],
        results: dict[str, Any],
        task_map: dict[str, asyncio.Task],
        models: dict[str, dict[str, Any]],
        graph: DependencyGraph,
        pending: set[str],
        ready: set[str],
        new_ready: set[str],
        exc_info=None,
    ):
        """
        Process a failed task.

        Args:
            task: The failed asyncio.Task
            error: The exception that occurred
            executing: Set of currently executing model names
            completed: Set of completed model names
            results: Dictionary to store execution results
            task_map: Dictionary mapping model names to asyncio.Task objects
            models: Dictionary of all model definitions
            graph: Dependency graph
            pending: Set of pending model names
            new_ready: Set to add newly ready models to
        """
        # CRITICAL: Mark failed model as complete so it doesn't block the loop
        # Find which model failed
        for model_name in list(executing):
            if model_name in task_map and task_map[model_name] == task:
                executing.remove(model_name)
                completed.add(model_name)  # Mark as complete even on error
                results[model_name] = {"status": "error", "model": model_name, "error": str(error)}
                if model_name in task_map:
                    del task_map[model_name]

                # Update task status and log error
                import traceback

                # Get full traceback for logging
                # Use exc_info if provided (from exception context), otherwise use error's traceback
                if exc_info and len(exc_info) == 3:
                    exc_type, exc_value, exc_tb = exc_info
                else:
                    exc_type, exc_value, exc_tb = type(error), error, error.__traceback__

                error_traceback = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))

                if model_name in self.flow.tasks:
                    flow_task = self.flow.tasks[model_name]
                    flow_task.complete(success=False)
                    flow_task.error_message = str(error)  # Short message for progress bar
                    flow_task.error_stacktrace = error_traceback

                # Log error with full traceback - this will appear in logs above progress bars and in file
                error_message = f"Model '{model_name}' failed: {str(error)}"

                # Log error with exception info tuple (preserves full traceback)
                # Always use exc_info tuple if we have it (from sys.exc_info())
                # This ensures the file handler receives the full traceback
                try:
                    if exc_info and len(exc_info) == 3:
                        # Use provided exc_info tuple (from sys.exc_info())
                        exc_type, exc_value, exc_tb = exc_info
                        logger.error(
                            error_message,
                            exc_info=(exc_type, exc_value, exc_tb),
                            extra={"model_name": model_name},
                        )
                    elif exc_tb is not None:
                        # Use constructed exc_info from error object
                        logger.error(
                            error_message,
                            exc_info=(exc_type, exc_value, exc_tb),
                            extra={"model_name": model_name},
                        )
                    else:
                        # Fallback - log without traceback if we can't get it
                        # Don't use exc_info=True here as we're outside exception context
                        logger.error(error_message, extra={"model_name": model_name})
                except Exception as log_error:
                    # If logging fails, at least try to log a simple error message
                    # This should never happen, but if it does, we want to know
                    try:
                        logger.error(f"Model '{model_name}' failed: {str(error)} (logging error: {log_error})")
                    except Exception:
                        # Last resort - print to stderr
                        import sys

                        # Fallback: use stderr if logging completely fails

                        sys.stderr.write(f"ERROR: Model '{model_name}' failed: {str(error)}\n")
                        sys.stderr.write(f"ERROR: Logging also failed: {log_error}\n")

                # Also ensure error is added to display for error panel
                if self.display.enabled:
                    self.display.add_error(model_name, f"{str(error)}\n{error_traceback}")

                # Update display
                self.display.update_from_flow()

                # Mark dependents as failed since their dependency failed
                # Don't add failed model to succeeded set - it failed
                # If a dependent has this failed model as a dependency, it cannot succeed
                dependents = graph.get_dependents(model_name)
                for dependent in dependents:
                    # Skip dependents that are already executing or completed
                    if dependent in executing or dependent in completed:
                        continue

                    # Mark as skipped if in pending or ready (not yet executing)
                    if dependent in pending or dependent in ready:
                        # Remove from pending/ready sets
                        if dependent in pending:
                            pending.remove(dependent)
                        if dependent in ready:
                            ready.discard(dependent)  # Use discard to avoid KeyError if not present

                        completed.add(dependent)  # Mark as complete (skipped)
                        # Don't add to succeeded - it was skipped

                        # Create error message indicating the failed dependency
                        error_msg = f"Dependency '{model_name}' failed: {str(error)}"

                        # Check if this dependent already has an error (multiple failed dependencies)
                        if dependent in results and results[dependent].get("status") == "error":
                            # Append to existing error
                            existing_error = results[dependent].get("error", "")
                            error_msg = f"{existing_error}; {error_msg}"
                            failed_deps = results[dependent].get("failed_dependencies", [])
                            if model_name not in failed_deps:
                                failed_deps.append(model_name)
                        else:
                            failed_deps = [model_name]

                        results[dependent] = {
                            "status": "error",
                            "model": dependent,
                            "error": error_msg,
                            "failed_dependencies": failed_deps,
                        }

                        # Update task status - mark as skipped since dependency failed
                        if dependent in self.flow.tasks:
                            dep_task = self.flow.tasks[dependent]
                            dep_task.skip()  # Mark as skipped, not failed
                            dep_task.error_message = error_msg
                            dep_task.error_stacktrace = (
                                f"Dependency '{model_name}' failed with error:\n{error_traceback}"
                            )

                            if self.state_store:
                                self.state_store.save_task(dep_task)

                        # Log error for dependent model
                        logger.error(
                            f"Model '{dependent}' skipped: {error_msg}",
                            extra={"model_name": dependent, "failed_dependency": model_name},
                        )

                        # Add error to display
                        if self.display.enabled:
                            self.display.add_error(
                                dependent,
                                f"Skipped due to failed dependency '{model_name}': {str(error)}",
                            )

                        # Update display immediately
                        self.display.update_from_flow()
                break

    def _create_table_safe(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        obj: Any,
        schema: str,
        overwrite: bool = False,
    ) -> None:
        """
        Create a table safely, handling schema/qualified name variations.

        Tries to create table with database parameter first, falls back to qualified name
        if that fails (for backends that don't support database parameter).

        Args:
            connection: ibis connection backend
            table_name: Name of the table to create
            obj: Data to create table from (DataFrame, ibis.Table, etc.)
            schema: Schema/database name
            overwrite: Whether to overwrite existing table
        """
        try:
            connection.create_table(table_name, obj=obj, database=schema, overwrite=overwrite)
        except (TypeError, AttributeError):
            # Fallback: use qualified name for backends that don't support database parameter
            qualified_name = f"{schema}.{table_name}"
            connection.create_table(qualified_name, obj=obj, overwrite=overwrite)

    def _check_table_exists(self, connection: ibis.BaseBackend, table_name: str, schema: str | None = None) -> bool:
        """
        Check if a table exists using cached checks.

        Args:
            connection: ibis connection backend
            table_name: Table name to check
            schema: Schema/database name (optional)

        Returns:
            True if table exists, False otherwise
        """
        cache_key = (table_name, schema)
        if cache_key in self._table_existence_cache:
            return self._table_existence_cache[cache_key]

        exists = check_table_exists(connection, table_name, schema)
        self._table_existence_cache[cache_key] = exists

        # Invalidate schema cache if table existence changed
        # This handles cases where table is dropped/recreated externally
        schema_cache_key = f"{schema or 'main'}.{table_name}"
        if schema_cache_key in self._schema_cache:
            # If table doesn't exist but we have cached schema, invalidate it
            if not exists:
                del self._schema_cache[schema_cache_key]

        return exists


# Helper functions for schema validation
# Note: _table_exists has been moved to interlace.utils.table_utils.check_table_exists


def _ibis_type_to_sql(ibis_type: dt.DataType) -> str:
    """Convert ibis DataType to SQL type string."""
    type_str = str(ibis_type)
    # Map ibis types to SQL types
    type_map = {
        "int8": "BIGINT",
        "int16": "SMALLINT",
        "int32": "INTEGER",
        "int64": "BIGINT",
        "float32": "REAL",
        "float64": "DOUBLE",
        "string": "VARCHAR",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "time": "TIME",
        "timestamp": "TIMESTAMP",
    }
    # Extract base type (remove nullable info)
    base_type = type_str.split("(")[0].split("!")[0].lower()
    return type_map.get(base_type, "VARCHAR")


def _is_safe_type_cast(from_type: dt.DataType, to_type: dt.DataType) -> bool:
    """Check if type cast is safe (won't lose data)."""
    # For Phase 0, simple checks
    # Same type is always safe
    if from_type == to_type:
        return True

    # Integer widening is safe
    int_types = ["int8", "int16", "int32", "int64"]
    from_str = str(from_type).lower()
    to_str = str(to_type).lower()

    if any(t in from_str for t in int_types) and any(t in to_str for t in int_types):
        # Check if widening
        if "int8" in from_str and "int16" in to_str:
            return True
        if "int16" in from_str and "int32" in to_str:
            return True
        if "int32" in from_str and "int64" in to_str:
            return True

    # VARCHAR to TEXT is safe
    if "string" in from_str or "varchar" in from_str:
        if "string" in to_str or "varchar" in to_str or "text" in to_str:
            return True

    # Default: conservative - require manual migration
    return False


async def execute_models(
    models: dict[str, dict[str, Any]],
    graph: DependencyGraph,
    config: dict[str, Any],
    use_progress: bool = True,
    force: bool = False,
    since: str | None = None,
    until: str | None = None,
) -> dict[str, Any]:
    """
    Execute models using dynamic parallel execution.

    Phase 0: Basic execution wrapper.

    Args:
        models: Dictionary of model definitions
        graph: Dependency graph
        config: Configuration dictionary
        use_progress: Whether to show Rich progress display (default: True)
        force: Force execution (bypass change detection)
        since: Backfill lower bound - overrides cursor start value
        until: Backfill upper bound - adds cursor upper-bound filter
    """
    # Inject backfill overrides into config dict for Executor to pick up
    if since is not None:
        config["_since"] = since
    if until is not None:
        config["_until"] = until

    executor = Executor(config)  # Progress display is handled internally
    try:
        return await executor.execute_dynamic(models, graph, force=force)
    finally:
        # Cleanup thread pool executor
        executor.executor_pool.shutdown(wait=False)
