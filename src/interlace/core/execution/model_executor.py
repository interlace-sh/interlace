"""
Model execution for Python and SQL models.

Phase 0: Extracted from Executor class for better separation of concerns.
Phase 2: Added retry framework integration for transient failure handling.
"""

import asyncio
import inspect
import time
import traceback
import sys
from typing import Dict, Any, Optional, Callable
import ibis
from interlace.core.context import set_connection, get_connection as get_context_connection
from interlace.utils.table_utils import check_table_exists
from interlace.core.execution.config import ModelExecutorConfig
from interlace.utils.logging import get_logger

logger = get_logger("interlace.execution.model_executor")


def _max_cursor_value(*values: str) -> str:
    """Return the maximum cursor value, preferring numeric comparison.

    All cursor values are stored as strings, but ``"9" > "100"`` under
    lexicographic ordering.  This helper attempts to parse each value as a
    number so that ``"100"`` is correctly considered greater than ``"9"``.

    Falls back to string comparison when values are non-numeric (e.g.
    ISO-8601 timestamps, which compare correctly as strings).
    """
    try:
        numeric = [float(v) for v in values]
        idx = numeric.index(max(numeric))
        return values[idx]
    except (ValueError, TypeError):
        return max(values)


class ModelExecutor:
    """
    Executes individual models (Python and SQL).

    Phase 0: Handles execution of Python and SQL models, including dependency
    loading, schema validation, and materialization coordination.

    Phase 2: Integrates retry framework for handling transient failures with
    exponential backoff, circuit breaker, and dead letter queue support.
    """

    def __init__(self, config: ModelExecutorConfig):
        """
        Initialize ModelExecutor.

        Uses configuration object to reduce coupling and simplify constructor.

        Args:
            config: ModelExecutorConfig with all required dependencies and callbacks
        """
        self.change_detector = config.change_detector
        self.connection_manager = config.connection_manager
        self.dependency_loader = config.dependency_loader
        self.schema_manager = config.schema_manager
        self.materialization_manager = config.materialisation_manager
        self.data_converter = config.data_converter
        self.materializers = config.materialisers
        self.materialised_tables = config.materialised_tables
        self.executor_pool = config.executor_pool
        self.state_store = config.state_store
        self.flow = config.flow
        self.display = config.display
        self._get_row_count = config.get_row_count_func
        self._update_model_last_run = config.update_model_last_run_func
        self._log_model_end = config.log_model_end_func
        self._prepare_model_execution = config.prepare_model_execution_func
        self._store_materialised_table = config.store_materialised_table_func

        # Phase 2: Retry framework components
        self.retry_manager = config.retry_manager
        self.dlq = config.dlq

        # Backfill overrides
        self.since = config.since
        self.until = config.until

        # Timing tracking for debugging parallelization
        self.model_timing: Dict[str, Dict[str, Any]] = {}

    async def execute_model(
        self, model_name: str, model_info: Dict[str, Any], models: Dict[str, Dict[str, Any]], force: bool = False
    ) -> Any:
        """
        Execute a single model.
        
        Includes resource monitoring if enabled.
        
        Handles both Python and SQL models. Uses model's @connection parameter to determine
        which connection to use for materialization.

        Args:
            model_name: Model name
            model_info: Model info dict
            models: All models dict
            force: Force execution (bypass change detection)
        """
        # Check if model should run (change detection)
        if not force:
            should_run, reason = await self.change_detector.should_run_model(model_name, model_info, models, force)
            if not should_run:
                logger.debug(f"Model '{model_name}' skipped: {reason}")
                # Return success but with skipped status
                return {"status": "skipped", "model": model_name, "reason": reason, "rows": None, "elapsed": 0.0}

        # Check cache policy (Phase 3: source caching with TTL)
        if not force:
            cache_skip = self._check_cache_policy(model_name, model_info)
            if cache_skip:
                logger.info(f"Model '{model_name}' skipped: {cache_skip}")
                return {"status": "skipped", "model": model_name, "reason": cache_skip, "rows": None, "elapsed": 0.0}

        # Prepare execution
        model_type, materialise, schema, strategy_name, start_time = self._prepare_model_execution(
            model_name, model_info
        )

        # Get model's connection (defaults to default connection)
        model_conn_name = model_info.get("connection") or self.connection_manager._default_conn_name
        
        # Initialize connection variables for cleanup
        model_conn = None
        conn_obj = None
        is_pooled = False

        # Create a NEW connection for this task (critical for DuckDB parallel execution)
        # DuckDB connections are NOT thread-safe, so sharing causes serialization
        # Creating per-task connections allows true parallel execution
        # For Postgres, uses connection pooling if enabled
        try:
            model_conn = await self.connection_manager.create_task_connection(model_conn_name)
            
            # Track connection info for cleanup (needed for Postgres pool return)
            conn_obj = self.connection_manager.connections.get(model_conn_name)
            is_pooled = conn_obj and hasattr(conn_obj, "return_pooled_connection")
            
            # Set connection in context early so models can access it
            set_connection(model_conn)
        except Exception as e:
            # Re-raise with clear error message - don't fall back to avoid confusion
            raise ValueError(
                f"Could not create connection '{model_conn_name}' for model '{model_name}': {e}. "
                f"If this is a file lock error, close other processes accessing the database. "
                f"Available connections: {list(self.connection_manager._connection_configs.keys())}"
            ) from e

        try:
            # Load dependencies
            dependency_tables = await self.dependency_loader.load_model_dependencies(
                model_info, model_conn, models, schema
            )

            # Phase 6: Cursor-based incremental filtering
            # If the model has a cursor parameter, filter each dependency
            # to only include rows with cursor_column > last_processed_value
            cursor_column = model_info.get("cursor")
            cursor_new_values = {}  # dep_name -> new max cursor value to save
            skip_cursor_save = False  # True during backfill to preserve real watermark
            if cursor_column and dependency_tables:
                last_value = None

                # Backfill override: --since replaces stored cursor watermark
                if self.since is not None:
                    last_value = self.since
                    skip_cursor_save = True
                    logger.debug(
                        f"Backfill: using --since={self.since} as cursor lower bound "
                        f"for '{model_name}' (cursor save suppressed)"
                    )
                elif self.state_store:
                    last_value = self.state_store.get_cursor_value(model_name)
                    if last_value is not None:
                        logger.debug(
                            f"Cursor '{cursor_column}' for '{model_name}': "
                            f"last_processed_value={last_value}"
                        )

                for dep_name, dep_table in list(dependency_tables.items()):
                    if not isinstance(dep_table, ibis.Table):
                        continue
                    try:
                        dep_schema = dep_table.schema()
                    except Exception:
                        continue
                    if cursor_column not in dep_schema.names:
                        continue

                    # Capture new max cursor value BEFORE filtering
                    try:
                        max_val = dep_table.select(
                            dep_table[cursor_column].max().name("_max_cursor")
                        ).execute()
                        if max_val is not None and len(max_val) > 0:
                            new_max = max_val.iloc[0, 0]
                            if new_max is not None:
                                cursor_new_values[dep_name] = str(new_max)
                    except Exception as e:
                        logger.debug(
                            f"Could not compute max({cursor_column}) on "
                            f"{dep_name}: {e}"
                        )

                    # Filter to only new rows (lower bound)
                    if last_value is not None:
                        col = dep_table[cursor_column]
                        try:
                            # Try numeric comparison first
                            numeric_val = float(last_value)
                            if numeric_val == int(numeric_val):
                                numeric_val = int(numeric_val)
                            dep_table = dep_table.filter(col > numeric_val)
                        except (ValueError, TypeError):
                            # Fall back to string comparison (timestamps, etc.)
                            dep_table = dep_table.filter(col > last_value)

                    # Backfill upper bound: --until adds col <= until_value
                    if self.until is not None:
                        col = dep_table[cursor_column]
                        try:
                            numeric_until = float(self.until)
                            if numeric_until == int(numeric_until):
                                numeric_until = int(numeric_until)
                            dep_table = dep_table.filter(col <= numeric_until)
                        except (ValueError, TypeError):
                            dep_table = dep_table.filter(col <= self.until)

                    dependency_tables[dep_name] = dep_table

            # Reset start_time to actual execution start (after dependencies loaded)
            # This gives accurate per-model timing, not cumulative timing
            execution_start_time = time.time()

            # Phase 2: Execute model with optional retry logic
            # Define core execution function that can be retried
            async def _core_execute():
                if model_type == "python":
                    return await self.execute_python_model(model_info, dependency_tables, model_conn)
                elif model_type == "sql":
                    return await self.execute_sql_model(model_info, dependency_tables, model_conn)
                elif model_type == "stream":
                    return await self.execute_stream_model(model_info, model_conn)
                else:
                    raise ValueError(f"Unknown model type: {model_type}")

            # Get retry policy from model config (Phase 2)
            retry_policy = model_info.get("retry_policy")

            # Execute with or without retry
            if retry_policy and self.retry_manager:
                # Execute with retry framework
                logger.debug(
                    f"Model '{model_name}' has retry policy: max_attempts={retry_policy.max_attempts}"
                )
                try:
                    result = await self.retry_manager.execute(
                        _core_execute,
                        policy=retry_policy,
                        model_name=model_name
                    )
                except Exception as e:
                    # All retries exhausted - add to DLQ if configured
                    if self.dlq and retry_policy.use_dlq:
                        from interlace.core.retry import DLQEntry
                        entry = DLQEntry(
                            model_name=model_name,
                            exception_type=type(e).__name__,
                            exception_message=str(e),
                            exception_traceback=traceback.format_exc(),
                            total_attempts=retry_policy.max_attempts + 1,
                            model_config={
                                "schema": schema,
                                "materialise": materialise,
                                "strategy": strategy_name,
                                "connection": model_conn_name,
                            },
                            model_dependencies=list(dependency_tables.keys()),
                        )
                        self.dlq.add(entry)
                    raise
            else:
                # Execute without retry (original behavior)
                result = await _core_execute()

            if result is None:
                elapsed = time.time() - execution_start_time
                if model_name in self.model_timing:
                    self.model_timing[model_name]["end_time"] = time.time()

                # Save cursor watermark on success (side-effect models)
                # Skip during backfill to preserve the real high-water mark
                if cursor_column and cursor_new_values and self.state_store and not skip_cursor_save:
                    # Use the highest new max across all dependencies
                    best_value = _max_cursor_value(*cursor_new_values.values())
                    self.state_store.save_cursor_value(
                        model_name, cursor_column, best_value
                    )
                    logger.debug(
                        f"Cursor '{cursor_column}' for '{model_name}' "
                        f"updated to {best_value}"
                    )

                task = None
                if self.flow and model_name in self.flow.tasks:
                    task = self.flow.tasks[model_name]
                    task.complete(success=True)
                    if self.state_store:
                        self.state_store.save_task(task)
                
                # Log model completion (no data returned - side-effect model)
                self._log_model_end(
                    model_name=model_name,
                    model_info=model_info,
                    success=True,
                    duration=elapsed,
                    rows_processed=None,
                )
                
                return {"status": "success", "model": model_name, "rows": None, "elapsed": elapsed}

            # Setup reference table for schema validation
            if not isinstance(result, ibis.Table):
                result = await self.schema_manager.setup_reference_table(result, model_name, model_conn, model_info)

            # Capture rows_ingested (input count before strategy)
            rows_ingested = self._get_row_count(result, model_name, materialise, schema, model_conn)
            if self.flow and model_name in self.flow.tasks:
                task = self.flow.tasks[model_name]
                task.rows_ingested = rows_ingested

            # Validate schema and update if needed (for table materialisation)
            # Schema validation happens BEFORE strategy application
            schema_changes_count = 0
            if materialise == "table":
                try:
                    schema_changes_count = await self.schema_manager.validate_and_update_schema(
                        result, model_name, schema, model_conn, model_info
                    )
                except Exception as e:
                    # Schema validation failures are non-fatal - log but continue
                    logger.debug(f"Schema validation failed for {model_name} (non-fatal): {e}")
                    schema_changes_count = 0
                
                if self.flow and model_name in self.flow.tasks:
                    task = self.flow.tasks[model_name]
                    task.schema_changes = schema_changes_count

                # Update display after schema changes
                if self.display:
                    self.display.update_from_flow()

            # Apply strategy and materialise
            # Update task status to materialising
            if self.flow and model_name in self.flow.tasks:
                task = self.flow.tasks[model_name]
                task.mark_materialising()
                if self.state_store:
                    self.state_store.save_task(task)
                # Update display to show materialising status
                if self.display:
                    self.display.update_from_flow()
            
            materializer = self.materializers.get(materialise)
            if materializer:
                if materialise == "table" and strategy_name:
                    await self.materialization_manager.materialise_table_with_strategy(
                        result, model_name, schema, strategy_name, model_info, model_conn
                    )
                else:
                    # Pass fields, file_path, and state_connection to materializer
                    fields = model_info.get("fields")
                    file_path = model_info.get("file")
                    # Pass state connection for view change detection
                    state_conn = None
                    if self.state_store:
                        try:
                            state_conn = self.state_store._get_connection()
                        except Exception:
                            pass
                    # Pass kwargs to materializer
                    materialize_kwargs = {"fields": fields}
                    if file_path:
                        materialize_kwargs["file_path"] = file_path
                    if state_conn:
                        materialize_kwargs["state_connection"] = state_conn
                    await self.materialization_manager.materialise(
                        materializer, result, model_name, schema, materialise, model_conn, **materialize_kwargs
                    )
            elif materialise == "none":
                self.materialised_tables[model_name] = result

            # Store materialised table for downstream models
            # Wrap in try/except to ensure table loading failures don't fail the task
            try:
                await self._store_materialised_table(model_name, result, materialise, schema, model_conn)
            except Exception as e:
                # If storing fails (e.g., table not found when loading), log but don't fail
                # The result table is already stored in materialised_tables by _store_materialised_table
                logger.debug(f"Could not store materialised table {model_name} (non-fatal): {e}")

            # Get row count from materialised table
            rows = self._get_row_count(result, model_name, materialise, schema, model_conn)

            # Export to file if configured
            export_config = model_info.get("export")
            if export_config and isinstance(export_config, dict) and materialise != "ephemeral":
                try:
                    from interlace.export import export_table
                    export_path = export_table(model_conn, model_name, schema, export_config)
                    logger.info(f"Exported {model_name} to {export_path}")
                except Exception as e:
                    logger.warning(f"Export failed for {model_name}: {e}")

            # Save cursor watermark on success (data-returning models)
            # Skip during backfill to preserve the real high-water mark
            if cursor_column and cursor_new_values and self.state_store and not skip_cursor_save:
                best_value = _max_cursor_value(*cursor_new_values.values())
                self.state_store.save_cursor_value(
                    model_name, cursor_column, best_value
                )
                logger.debug(
                    f"Cursor '{cursor_column}' for '{model_name}' "
                    f"updated to {best_value}"
                )

            elapsed = time.time() - execution_start_time
            if model_name in self.model_timing:
                self.model_timing[model_name]["end_time"] = time.time()

            # Update task with results
            task = None
            if self.flow and model_name in self.flow.tasks:
                task = self.flow.tasks[model_name]
                task.rows_processed = rows
                task.complete(success=True)
                if self.state_store:
                    self.state_store.save_task(task)

            # Update last_run_at timestamp
            if self.state_store:
                try:
                    state_conn = self.state_store._get_connection()
                    if state_conn:
                        self._update_model_last_run(state_conn, model_name, schema)
                except Exception as e:
                    logger.debug(f"Could not update last_run_at for {model_name}: {e}")

            # Log model completion with statistics
            self._log_model_end(
                model_name=model_name,
                model_info=model_info,
                success=True,
                duration=elapsed,
                rows_processed=rows,
                rows_ingested=task.rows_ingested if task else None,
                rows_inserted=task.rows_inserted if task else None,
                rows_updated=task.rows_updated if task else None,
                rows_deleted=task.rows_deleted if task else None,
                schema_changes=task.schema_changes if task else 0,
            )

            return {"status": "success", "model": model_name, "rows": rows, "elapsed": elapsed}

        except Exception as e:
            # Use execution_start_time if available, otherwise fall back to start_time
            # (execution_start_time is set after dependencies are loaded)
            try:
                elapsed = time.time() - execution_start_time
            except NameError:
                # Fallback if execution_start_time wasn't set (shouldn't happen, but be safe)
                elapsed = time.time() - start_time
            
            if model_name in self.model_timing:
                self.model_timing[model_name]["end_time"] = time.time()
            
            # Log model failure with timing
            self._log_model_end(
                model_name=model_name,
                model_info=model_info,
                success=False,
                duration=elapsed,
                error=str(e),
            )
            
            # Capture the full traceback from the original exception
            # Skip the asyncio wrapper traceback if present - use the underlying error instead
            exc_type, exc_value, exc_tb = sys.exc_info()
            
            # If the exception is from async_utils.py (the wrapper), use the __context__ instead
            # This gives us the actual error location, not the async wrapper
            if exc_tb and "async_utils.py" in str(exc_tb.tb_frame.f_code.co_filename):
                # This is the async wrapper error - use the context (the real error) instead
                if exc_value.__context__:
                    # Use the context exception (the real error)
                    real_exc_type = type(exc_value.__context__)
                    real_exc_value = exc_value.__context__
                    real_exc_tb = real_exc_value.__traceback__
                    exc_type, exc_value, exc_tb = real_exc_type, real_exc_value, real_exc_tb
            
            # Filter traceback to show only user model code frames
            # Skip: interlace internals, site-packages (ibis, etc.), async wrappers
            def _is_user_frame(filename: str) -> bool:
                """Return True if this frame is from user model code."""
                if "site-packages" in filename:
                    return False
                if "async_utils.py" in filename:
                    return False
                if "interlace" in filename and ("pkg" in filename or "src/interlace" in filename):
                    return False
                if filename.startswith("<"):
                    return False
                return True

            # Collect user code frames for display
            user_frames = []
            tb = exc_tb
            while tb is not None:
                filename = tb.tb_frame.f_code.co_filename
                if _is_user_frame(filename):
                    user_frames.append(tb)
                tb = tb.tb_next

            if user_frames:
                # Build a clean traceback showing only user frames + error
                lines = ["Traceback (most recent call last):\n"]
                for frame_tb in user_frames:
                    lines.extend(traceback.format_tb(frame_tb, limit=1))
                lines.extend(traceback.format_exception_only(exc_type, exc_value))
                error_traceback = ''.join(lines)

                # Point exc_info at the last user frame for Rich formatting
                filtered_exc = exc_type(str(exc_value))
                filtered_exc.__traceback__ = user_frames[-1]
                exc_value = filtered_exc
                exc_tb = user_frames[-1]
            else:
                # No user frames found - show just the error type and message
                error_traceback = ''.join(traceback.format_exception_only(exc_type, exc_value))
            
            # Error is stored in Flow/Task object via display.add_error()
            # Store both the traceback string and the exception info for Rich formatting
            return {
                "status": "error",
                "model": model_name,
                "error": str(e),
                "traceback": error_traceback,
                "exc_info": (exc_type, exc_value, exc_tb),  # Store for Rich formatting
                "elapsed": elapsed
            }
        finally:
            # Clean up connection to prevent resource leaks
            # Note: For DuckDB, connections are lightweight but should still be closed
            # For Postgres with pooling, return connection to pool instead of closing
            if model_conn is not None:
                try:
                    if is_pooled and conn_obj:
                        # Return to pool (async)
                        await conn_obj.return_pooled_connection(model_conn)
                    elif hasattr(model_conn, "close"):
                        # Close directly (DuckDB and non-pooled connections)
                        model_conn.close()
                except Exception as close_error:
                    # Log but don't fail - connection cleanup is best effort
                    logger.debug(f"Error closing/returning connection for {model_name}: {close_error}")

    async def execute_python_model(
        self,
        model_info: Dict[str, Any],
        dependency_tables: Dict[str, ibis.Table],
        connection: ibis.BaseBackend,
    ) -> Optional[ibis.Table]:
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
            # Apply column mapping (renames) to ibis.Table if provided
            column_mapping = model_info.get("column_mapping")
            if column_mapping:
                result = self.data_converter._apply_mapping_to_ibis_table(result, column_mapping)
            return result
        else:
            # Get fields and schema options from model_info
            fields = model_info.get("fields")
            strict = model_info.get("strict", False)
            column_mapping = model_info.get("column_mapping")

            return self.data_converter.convert_to_ibis_table(
                result,
                fields=fields,
                strict=strict,
                column_mapping=column_mapping,
            )

    async def execute_sql_model(
        self,
        model_info: Dict[str, Any],
        dependency_tables: Dict[str, ibis.Table],
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
        # Performance: Try creating temp table directly from ibis.Table (avoids DataFrame materialization)
        # Only fall back to DataFrame if backend doesn't support direct ibis.Table creation
        for dep_name, dep_table in dependency_tables.items():
            try:
                if not check_table_exists(connection, dep_name, None):
                    try:
                        # Try direct creation from ibis.Table (more efficient - no materialization)
                        connection.create_table(dep_name, obj=dep_table, temp=True)
                    except (TypeError, AttributeError, NotImplementedError):
                        # Backend doesn't support direct ibis.Table - fall back to DataFrame
                        df = dep_table.execute()
                        connection.create_table(dep_name, obj=df, temp=True)
            except Exception as create_error:
                # Log warning - SQL query may fail if this dependency is referenced
                logger.warning(
                    f"Failed to create temp table for dependency '{dep_name}': {create_error}. "
                    f"SQL query may fail if this table is referenced."
                )

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
            # Performance: Use limit(0) for cheap validation (schema only, no data fetch)
            # Then return the same result expression (no re-parsing)
            try:
                _ = result.limit(0).execute()  # Cheap validation - only fetches schema
                return result  # Return validated expression directly (no re-parsing)
            except Exception as exec_error:
                error_msg = str(exec_error).lower()
                # Check if it's a duplicate CTE error
                if "duplicate cte" in error_msg or (
                    "duplicate" in error_msg and "cte" in error_msg
                ):
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

    async def execute_stream_model(
        self,
        model_info: Dict[str, Any],
        connection: ibis.BaseBackend,
    ) -> None:
        """
        Ensure stream table exists (append-only ingestion table).

        Streams are not executed like normal models; ingestion happens via service endpoints.
        This function ensures the table exists (optionally with provided fields schema).
        """
        model_name = model_info.get("name")
        schema = model_info.get("schema", "events")
        if not model_name:
            raise ValueError("Stream model missing name")

        # Ensure schema exists
        try:
            if hasattr(connection, "create_database"):
                connection.create_database(schema, force=False)
        except Exception:
            pass

        # If table exists already, nothing to do.
        try:
            if hasattr(connection, "list_tables"):
                existing = connection.list_tables(database=schema)
                if model_name in existing:
                    return None
        except Exception:
            pass        # Create an empty table using fields schema if provided.
        fields = model_info.get("fields")
        if fields:
            try:
                from interlace.utils.schema_utils import fields_to_ibis_schema

                ibis_schema = fields_to_ibis_schema(fields)
                if ibis_schema:
                    connection.create_table(
                        model_name,
                        schema=ibis_schema,
                        database=schema,
                        overwrite=False,
                    )
                    return None
            except Exception:
                # Fall back to creating no-op table below
                pass

        # Last resort: create a minimal table with a single column.
        try:
            connection.create_table(
                model_name,
                schema=ibis.schema({"_placeholder": "string"}),
                database=schema,
                overwrite=False,
            )
        except Exception:
            # If even this fails, just return; downstream ingestion will create it.
            return None

        return None

    def _check_cache_policy(self, model_name: str, model_info: Dict[str, Any]) -> Optional[str]:
        """
        Check if a model should be skipped due to cache policy.

        Phase 3: Source caching for models that fetch from external APIs.
        Models with a ``cache`` config can skip execution when their data
        is still fresh (within TTL) or when their materialised table exists.

        Cache strategies:
            - ``"ttl"``: Skip if last successful run was within the TTL period.
            - ``"if_exists"``: Skip if the materialised table already exists.
            - ``"always"``: Never skip (normal behaviour).

        Args:
            model_name: Name of the model
            model_info: Model metadata dict (includes ``cache`` key)

        Returns:
            Skip reason string if model should be skipped, None otherwise
        """
        cache_config = model_info.get("cache")
        if not cache_config:
            return None

        strategy = cache_config.get("strategy", "ttl")

        if strategy == "always":
            return None

        if strategy == "if_exists":
            # Check if the materialised table exists
            schema_name = model_info.get("schema", "public")
            model_conn_name = model_info.get("connection") or self.connection_manager._default_conn_name
            conn_obj = self.connection_manager.connections.get(model_conn_name)
            if conn_obj and hasattr(conn_obj, "connection"):
                try:
                    if check_table_exists(conn_obj.connection, model_name, schema_name):
                        return f"cache policy 'if_exists': table '{schema_name}.{model_name}' already exists"
                except Exception:
                    pass
            return None

        if strategy == "ttl":
            ttl_str = cache_config.get("ttl")
            if not ttl_str:
                return None

            from interlace.core.model import parse_ttl

            try:
                ttl_seconds = parse_ttl(ttl_str)
            except ValueError as e:
                logger.warning(f"Model '{model_name}': invalid cache TTL: {e}")
                return None

            # Check last successful run time from state store
            if self.state_store:
                try:
                    schema_name = model_info.get("schema", "public")
                    last_run = self.state_store.get_model_last_run_at(model_name, schema_name)
                    if last_run:
                        elapsed = time.time() - last_run.timestamp()
                        if elapsed < ttl_seconds:
                            remaining = ttl_seconds - elapsed
                            # Format remaining time
                            if remaining > 86400:
                                remaining_str = f"{remaining / 86400:.1f}d"
                            elif remaining > 3600:
                                remaining_str = f"{remaining / 3600:.1f}h"
                            elif remaining > 60:
                                remaining_str = f"{remaining / 60:.0f}m"
                            else:
                                remaining_str = f"{remaining:.0f}s"
                            return (
                                f"cache TTL '{ttl_str}' not expired "
                                f"({remaining_str} remaining)"
                            )
                except Exception as e:
                    logger.debug(f"Could not check cache TTL for '{model_name}': {e}")

            return None

        # Unknown strategy -- don't skip
        logger.warning(f"Model '{model_name}': unknown cache strategy '{strategy}'")
        return None
