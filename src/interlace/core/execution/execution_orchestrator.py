"""
Execution orchestration for parallel model execution.

Phase 0: Extracted from Executor class for better separation of concerns.
"""

import asyncio
import time
import traceback
from typing import Dict, Any, Set, Optional, TYPE_CHECKING, Callable
from interlace.core.dependencies import DependencyGraph
from interlace.core.flow import Flow, Task, TaskStatus
from interlace.utils.logging import get_logger

if TYPE_CHECKING:
    from interlace.core.execution.model_executor import ModelExecutor
    from interlace.core.execution.change_detector import ChangeDetector
    from interlace.core.execution.materialization_manager import MaterializationManager
    from interlace.core.state import StateStore
    from interlace.utils.display import RichDisplay

logger = get_logger("interlace.execution.execution_orchestrator")


class ExecutionOrchestrator:
    """
    Orchestrates parallel execution flow.
    
    Phase 0: Handles dynamic parallel execution of models, managing task lifecycle,
    dependency resolution, and error handling.
    """

    def __init__(
        self,
        model_executor: "ModelExecutor",
        change_detector: "ChangeDetector",
        materialization_manager: "MaterializationManager",
        state_store: Optional["StateStore"] = None,
        display: Optional["RichDisplay"] = None,
        config: Optional[Dict[str, Any]] = None,
        max_iterations: int = 10000,
        task_timeout: float = 3600.0,
        thread_pool_size: Optional[int] = None,
        executor_flow_setter: Optional[Callable[[Flow], None]] = None,
    ):
        """
        Initialize ExecutionOrchestrator.
        
        Args:
            model_executor: ModelExecutor instance
            change_detector: ChangeDetector instance
            materialization_manager: MaterializationManager instance
            state_store: Optional StateStore instance
            display: Optional RichDisplay instance
            config: Configuration dictionary
            max_iterations: Maximum loop iterations (safety check)
            task_timeout: Task timeout in seconds
            thread_pool_size: Thread pool size for display
            executor_flow_setter: Callback to set flow on Executor (for backward compatibility)
        """
        self.model_executor = model_executor
        self.change_detector = change_detector
        self.materialization_manager = materialization_manager
        self.state_store = state_store
        self.display = display
        self.config = config or {}
        self.max_iterations = max_iterations
        self.task_timeout = task_timeout
        self.thread_pool_size = thread_pool_size
        self.executor_flow_setter = executor_flow_setter
        self.flow = None
        self._force_execution = False

    async def execute_dynamic(
        self, models: Dict[str, Dict[str, Any]], graph: DependencyGraph, force: bool = False
    ) -> Dict[str, Any]:
        """
        Execute models dynamically as dependencies complete.

        Phase 0: Dynamic parallel execution - execute models as soon as dependencies are ready.

        Args:
            models: Dictionary of model definitions
            graph: Dependency graph
            force: Force execution (bypass change detection)
        """
        # Store force flag for use in _start_model_execution
        self._force_execution = force
        
        # Pre-populate file hash cache from models for performance
        # This avoids repeated database queries during change detection
        if self.state_store and not force:
            try:
                state_conn = self.state_store._get_connection()
                if state_conn:
                    from interlace.utils.hashing import get_stored_file_hash
                    for model_name, model_info in models.items():
                        schema_name = model_info.get("schema", "public")
                        cache_key = (model_name, schema_name)
                        if cache_key not in self.change_detector._file_hash_cache:
                            stored_hash = get_stored_file_hash(state_conn, model_name, schema_name)
                            if stored_hash:
                                self.change_detector._file_hash_cache[cache_key] = stored_hash
            except Exception as e:
                logger.debug(f"Could not pre-populate file hash cache: {e}")

        # Create flow for this execution
        trigger_type = self.config.get("trigger", {}).get("type", "cli")
        trigger_id = self.config.get("trigger", {}).get("id")
        self.flow = Flow(
            trigger_type=trigger_type,
            trigger_id=trigger_id,
            metadata={"models": list(models.keys())},
        )
        self.flow.start()
        # Update materialization manager and model executor with flow
        self.materialization_manager.flow = self.flow
        self.model_executor.flow = self.flow
        # Update Executor's flow reference (for backward compatibility with _prepare_model_execution)
        if self.executor_flow_setter:
            self.executor_flow_setter(self.flow)
        
        # Save flow to state database
        if self.state_store:
            self.state_store.save_flow(self.flow)
        
        # Start flow timing
        flow_start_time = time.time()

        pending = set(models.keys())
        executing = set()
        completed = set()  # All completed models (success or failure)
        succeeded = set()  # Only successfully completed models
        results = {}
        task_map: Dict[str, asyncio.Task] = {}

        # Get layer mapping from graph to order progress bars by execution layer
        model_layers = graph.get_layers()

        # Sort models by layer for progress display (models in same layer sorted alphabetically)
        sorted_models = sorted(models.keys(), key=lambda m: (model_layers.get(m, 999), m))

        # Initialize flow tasks for all models (ordered by layer)
        for model_name in sorted_models:
            deps = graph.get_dependencies(model_name)
            materialise_type = models[model_name].get("materialise", "table")
            strategy = models[model_name].get("strategy")
            schema = models[model_name].get("schema", "main")

            # Create task in flow
            task = Task(
                flow_id=self.flow.flow_id,
                model_name=model_name,
                schema_name=schema,
                dependencies=deps,
                materialise=materialise_type,
                strategy=strategy,
            )
            task.enqueue()
            task.mark_waiting()  # Initially waiting for dependencies
            self.flow.tasks[model_name] = task
            
            # Save task to state database
            if self.state_store:
                self.state_store.save_task(task)

        # Initialize RichDisplay with Flow
        self.display.set_flow(self.flow)
        
        # Print heading first (creates layout if needed)
        # Always call if enabled - print_heading will set config internally
        if self.display.enabled:
            # Get project name from top-level config, fallback to "Interlace Project"
            project_name = self.config.get("name", "Interlace Project")
            # Get environment from config metadata or trigger
            env = self.config.get("_env") or self.config.get("trigger", {}).get("env")
            # Get project directory from config metadata
            project_dir = self.config.get("_project_dir")
            self.display.print_heading(
                project_name, 
                config=self.config, 
                state_store=self.state_store, 
                models=models,
                env=env,
                project_dir=project_dir
            )
        
        # Initialize progress (updates layout with progress bars)
        # Pass thread pool size for dynamic task visibility
        self.display.initialize_progress(self.flow, models, graph, thread_pool_size=self.thread_pool_size)

        # Get initial ready models (no dependencies)
        ready = set(model_name for model_name in pending if not graph.get_dependencies(model_name))

        # Start progress display - enter Live context BEFORE any logging
        # This ensures all logs go to the layout, not directly to console
        try:
            # Use RichDisplay context manager - this starts Live display
            with self.display:
                # Log discovery and execution info after entering display context so it appears below header
                logger.info(f"Discovered {len(models)} models: {', '.join(models.keys())}")
                logger.info(f"Running all {len(models)} discovered models")
                
                results = await self.execute_loop(
                    models, graph, pending, executing, completed, succeeded, results, task_map, ready
                )
                
                # Log flow summary BEFORE exiting display context so it appears above progress
                # Always log, but only include relevant parts (failed/skipped only if > 0)
                if self.flow:
                    summary = self.flow.get_summary()
                    duration = summary.get("duration", 0)
                    completed_count = summary.get("completed", 0)
                    total = summary.get("total_tasks", 0)
                    failed = summary.get("failed", 0)
                    skipped = summary.get("skipped", 0)
                    
                    # Build message with only relevant parts
                    parts = [f"{completed_count}/{total} tasks succeeded"]
                    if failed > 0:
                        parts.append(f"{failed} failed")
                    if skipped > 0:
                        parts.append(f"{skipped} skipped")
                    
                    message = ", ".join(parts) + f" in {duration:.2f}s"
                    logger.info(f"Flow {self.flow.flow_id[:8]} completed: {message}")
        except (KeyboardInterrupt, asyncio.CancelledError):
            # Gracefully handle Ctrl+C
            # CancelledError can occur when KeyboardInterrupt is raised during asyncio operations
            logger.info("Execution interrupted by user (Ctrl+C)")

            # Cancel all running tasks
            for model_name, task in list(task_map.items()):
                if not task.done():
                    task.cancel()
                    # Update task status
                    if model_name in self.flow.tasks:
                        self.flow.tasks[model_name].status = TaskStatus.SKIPPED
                        self.display.update_from_flow()

            # Wait for tasks to cancel (with timeout to avoid hanging)
            # Use shield to prevent our cancellation wait from being cancelled
            if task_map:
                cancelled_tasks = [t for t in task_map.values() if not t.done()]
                if cancelled_tasks:
                    try:
                        # Shield the gather to prevent it from being cancelled
                        await asyncio.wait_for(
                            asyncio.shield(
                                asyncio.gather(*cancelled_tasks, return_exceptions=True)
                            ),
                            timeout=2.0  # Don't wait too long
                        )
                    except (asyncio.TimeoutError, asyncio.CancelledError, KeyboardInterrupt):
                        # If tasks don't cancel quickly or we get interrupted again, just continue
                        # Don't wait - exit immediately
                        pass
                    except Exception:
                        # Ignore any other exceptions during cancellation
                        pass

            # Complete flow with failure status
            if self.flow:
                self.flow.complete(success=False)
                if self.state_store:
                    self.state_store.save_flow(self.flow)

            # Return partial results instead of raising exception for clean exit
            return results

        # Complete flow (after display context exits)
        if self.flow:
            # Check if any tasks failed
            has_failures = any(t.status == TaskStatus.FAILED for t in self.flow.tasks.values())
            self.flow.complete(success=not has_failures)
            
            # Save completed flow to state database
            if self.state_store:
                self.state_store.save_flow(self.flow)
            
            # Final display update to show all tasks now that flow is complete
            if self.display.enabled:
                self.display.update_from_flow()
            
            # Note: Flow completion log is done inside display context above

        return results

    async def execute_loop(
        self,
        models: Dict[str, Dict[str, Any]],
        graph: DependencyGraph,
        pending: Set[str],
        executing: Set[str],
        completed: Set[str],
        succeeded: Set[str],
        results: Dict[str, Any],
        task_map: Dict[str, asyncio.Task],
        ready: Set[str],
    ) -> Dict[str, Any]:
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
                    await self.start_model_execution(model_name, models, graph, task_map)

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
                    logger.error(
                        f"Deadlock detected - models have missing dependencies: {missing_deps}"
                    )
                    # Mark models with missing dependencies as failed
                    for model_name, missing in missing_deps.items():
                        pending.remove(model_name)
                        completed.add(model_name)
                        results[model_name] = {
                            "status": "error",
                            "model": model_name,
                            "error": f"Missing dependencies: {', '.join(missing)}",
                        }
                        if model_name in self.flow.tasks:
                            self.flow.tasks[model_name].complete(success=False)
                            self.flow.tasks[model_name].error_message = f"Missing dependencies: {', '.join(missing)}"
                            if self.state_store:
                                self.state_store.save_task(self.flow.tasks[model_name])
                    break

            # Wait for at least one task to complete
            if executing:
                await self.wait_for_task_completion(
                    executing, task_map, results, models, graph, pending, ready, new_ready, completed, succeeded
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

    async def start_model_execution(
        self,
        model_name: str,
        models: Dict[str, Dict[str, Any]],
        graph: DependencyGraph,
        task_map: Dict[str, asyncio.Task],
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
        # Get force flag from orchestrator instance (passed to execute_dynamic)
        force = self._force_execution
        task = asyncio.create_task(self.model_executor.execute_model(model_name, models[model_name], models, force=force))
        task_map[model_name] = task

    async def wait_for_task_completion(
        self,
        executing: Set[str],
        task_map: Dict[str, asyncio.Task],
        results: Dict[str, Any],
        models: Dict[str, Dict[str, Any]],
        graph: DependencyGraph,
        pending: Set[str],
        ready: Set[str],
        new_ready: Set[str],
        completed: Set[str],
        succeeded: Set[str],
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
        except asyncio.TimeoutError:
            logger.error(f"Timeout waiting for tasks to complete. Executing: {executing}")
            # Cancel hanging tasks
            for task in executing_tasks:
                task.cancel()
            return

        # Process completed tasks
        for task in done:
            try:
                result = await task
                await self.process_completed_task(
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
                await self.process_failed_task(
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

    async def process_completed_task(
        self,
        task: asyncio.Task,
        result: Dict[str, Any],
        executing: Set[str],
        completed: Set[str],
        succeeded: Set[str],
        results: Dict[str, Any],
        task_map: Dict[str, asyncio.Task],
        models: Dict[str, Dict[str, Any]],
        graph: DependencyGraph,
        pending: Set[str],
        new_ready: Set[str],
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
                                logger.error(error_message, exc_info=exc_info, extra={'model_name': model_name})
                            elif error_traceback:
                                # Fallback: include traceback in message if exc_info not available
                                logger.error(f"{error_message}\n{error_traceback}", extra={'model_name': model_name})
                            else:
                                # No traceback available, just log the error message
                                logger.error(error_message, exc_info=True, extra={'model_name': model_name})
                            
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
                                import sys
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
                if self.display.enabled:
                    self.display.update_from_flow()

                # Check dependents - if all dependencies are complete, mark as ready
                dependents = graph.get_dependents(model_name)
                for dependent in dependents:
                    if dependent in completed or dependent in executing:
                        continue  # Already handled
                    
                    # Check if all dependencies are complete
                    deps = graph.get_dependencies(dependent)
                    if all(dep in completed for dep in deps):
                        # All dependencies complete - mark as ready
                        if dependent in pending:
                            new_ready.add(dependent)

                break  # Found the model, no need to continue

    async def process_failed_task(
        self,
        task: asyncio.Task,
        error: Exception,
        executing: Set[str],
        completed: Set[str],
        succeeded: Set[str],
        results: Dict[str, Any],
        task_map: Dict[str, asyncio.Task],
        models: Dict[str, Dict[str, Any]],
        graph: DependencyGraph,
        pending: Set[str],
        ready: Set[str],
        new_ready: Set[str],
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
                
                error_traceback = ''.join(traceback.format_exception(exc_type, exc_value, exc_tb))
                
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
                            extra={'model_name': model_name}
                        )
                    elif exc_tb is not None:
                        # Use constructed exc_info from error object
                        logger.error(
                            error_message,
                            exc_info=(exc_type, exc_value, exc_tb),
                            extra={'model_name': model_name}
                        )
                    else:
                        # Fallback - log without traceback if we can't get it
                        # Don't use exc_info=True here as we're outside exception context
                        logger.error(
                            error_message,
                            extra={'model_name': model_name}
                        )
                except Exception as log_error:
                    # If logging fails, at least try to log a simple error message
                    # This should never happen, but if it does, we want to know
                    try:
                        logger.error(f"Model '{model_name}' failed: {str(error)} (logging error: {log_error})")
                    except Exception:
                        # Last resort - print to stderr
                        import sys
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
                            flow_task = self.flow.tasks[dependent]
                            flow_task.skip()  # Use method to set completed_at timestamp
                            flow_task.error_message = error_msg
                            if self.state_store:
                                self.state_store.save_task(flow_task)

                break  # Found the model, no need to continue

