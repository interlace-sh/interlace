"""
Display manager for Interlace execution.

Manages the display of:
- Header section (project name, key info)
- Logs section (takes remaining space between header/footer)
- Footer section (flow progress and task progress, pinned to bottom)

This is a global singleton that can be accessed from anywhere.
"""

import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from rich.align import Align
from rich.console import Console, Group
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import BarColumn, Progress, RenderableType, SpinnerColumn, TaskID, TextColumn
from rich.syntax import Syntax
from rich.text import Text

from interlace.core.flow import Flow, FlowStatus, Task, TaskStatus

# Module-level logger for display debugging (uses separate logger to avoid recursion)
_display_logger = logging.getLogger("interlace.display.internal")


class LogHandler(RichHandler):
    """
    Custom RichHandler that uses Progress console for logging.

    This handler:
    - Renders logs using RichHandler's built-in capabilities (tracebacks, markup, colors)
    - Uses the Progress console which automatically displays logs above progress bars
    - No manual display updates needed - Rich Progress handles it automatically
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Don't set console here - we'll get it lazily from display.progress.console
        # This allows progress to be created after LogHandler initialization
        if not kwargs.get("console"):
            # Create a default console if none provided (fallback)
            from rich.console import Console

            kwargs["console"] = Console()

        super().__init__(*args, **kwargs)
        self._cached_console: Console | None = None  # Cache the progress console once found

    def _get_progress_console(self) -> Console | None:
        """Get the Progress console from display (lazy lookup)."""
        # Use cached console if available to avoid repeated lookups and potential deadlocks
        if self._cached_console is not None:
            try:
                from interlace.utils.display import get_display

                display = get_display()
                # Quick check: if display is enabled and progress exists, use cached console
                # Don't do deep validation to avoid potential deadlocks
                if display.enabled and display.progress:
                    return self._cached_console
            except Exception:
                # If we can't access display, use cached console anyway
                return self._cached_console

        # Try to get progress console, but use non-blocking approach
        try:
            from interlace.utils.display import get_display

            display = get_display()
            # Use progress console if available and display is active (live context entered)
            if display.enabled and display.progress and display.live:
                self._cached_console = display.progress.console
                return self._cached_console
            elif display.enabled and display.progress:
                # Progress exists but live not started yet - use progress console anyway
                self._cached_console = display.progress.console
                return self._cached_console
            elif display.enabled:
                # Display enabled but no progress yet - use display console
                self._cached_console = display.console
                return self._cached_console
        except Exception:
            # If we can't access display (e.g., lock contention), use fallback
            pass

        # Fallback to the console we were initialized with
        return self.console

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record - Progress console handles display automatically."""
        # CRITICAL: We must NOT raise exceptions here, as that would prevent
        # the record from propagating to other handlers (like FileHandler).
        # If we fail, we call handleError() which allows propagation to continue.
        #
        # IMPORTANT: This method may be called from any thread (logging handlers
        # can be called from any thread). We must be careful about accessing
        # display state to avoid deadlocks.
        #
        # CRITICAL ISSUE: If super().emit() blocks when writing to progress console,
        # the record never propagates to FileHandler. We need to ensure propagation
        # happens even if console logging blocks.
        #
        # SOLUTION: In Python logging, handlers on the same logger are called sequentially.
        # If one handler blocks, others don't get the record. So if LogHandler.emit() blocks,
        # FileHandler.emit() never gets called.
        #
        # WORKAROUND: Use cached console to avoid accessing display state, and if console
        # logging fails or might block, skip it gracefully. The record will still propagate
        # to FileHandler because we don't raise exceptions - we just skip console output.
        try:
            # Use cached console if available - this avoids accessing display state
            # which might be locked by update_from_flow()
            if self._cached_console is not None:
                original_console = self.console
                self.console = self._cached_console
                try:
                    # Try to emit to console - if it blocks, we're stuck, but at least
                    # we're using cached console so we shouldn't deadlock
                    super().emit(record)
                except Exception:
                    # If console emit fails, continue anyway - record will still propagate
                    pass
                finally:
                    self.console = original_console
            else:
                # No cached console - try to get it, but if it fails, skip console logging
                # This ensures file handler still gets the record
                try:
                    progress_console = self._get_progress_console()
                    if progress_console and progress_console != self.console:
                        original_console = self.console
                        self.console = progress_console
                        try:
                            super().emit(record)
                        except Exception:
                            # If console emit fails, continue anyway
                            pass
                        finally:
                            self.console = original_console
                    else:
                        # Fallback to default console
                        try:
                            super().emit(record)
                        except Exception:
                            # If even default console fails, continue anyway
                            pass
                except Exception:
                    # If getting progress console fails (e.g., deadlock), skip console logging
                    # The record will still propagate to FileHandler
                    pass

            # Handle errors for model error tracking AFTER emitting
            # This ensures the record is processed by all handlers first
            # NOTE: We do this AFTER super().emit() to avoid any potential blocking
            if record.levelno >= logging.ERROR:
                try:
                    self._handle_error(record)
                except Exception:
                    # Don't break logging if error tracking fails
                    pass
        except (KeyboardInterrupt, SystemExit):
            # Re-raise these so they propagate properly
            raise
        except Exception:
            # If emit fails, call handleError to ensure logging continues
            # This is critical - if we don't call handleError, the record won't propagate
            # to other handlers (like FileHandler)
            self.handleError(record)
            # Don't print to stderr here as it might cause issues - just silently fail
            # The error will still be logged to file via handleError()

    def _handle_error(self, record: logging.LogRecord) -> None:
        """Extract model name from error record and add to display error panel."""
        try:
            # Try to extract model name from record extra (set by executor)
            model_name = getattr(record, "model_name", None)

            # Fallback: Try to extract model name from message (e.g., "Model 'users' failed: ...")
            if not model_name:
                # Use record.msg directly to avoid any potential side effects from getMessage()
                # record.msg is the raw message format string, record.args are the arguments
                msg = record.msg % record.args if record.args else record.msg
                if isinstance(msg, str) and "Model '" in msg and "' failed" in msg:
                    start = msg.find("Model '") + 7
                    end = msg.find("'", start)
                    if end > start:
                        model_name = msg[start:end]

            # If we have a model name, add to display errors (for error panel)
            if model_name:
                try:
                    from interlace.utils.display import get_display

                    display = get_display()
                    if display.enabled:
                        # Get full error message including traceback if available
                        # Build message without calling getMessage() to avoid any side effects
                        error_msg = record.msg % record.args if record.args else record.msg
                        if record.exc_info:
                            import traceback

                            error_msg += "\n" + "".join(traceback.format_exception(*record.exc_info))
                        display.add_error(model_name, error_msg)
                except Exception:
                    # Silently fail if display not available
                    pass
        except Exception:
            # Don't break logging if error tracking fails
            pass


class StatusColumn(TextColumn):
    """Column to show task status (tick/cross)."""

    # Fixed width for status column (all emojis are single character, but we want consistent spacing)
    STATUS_WIDTH = 1

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render status emoji with fixed width."""
        status = task.fields.get("status", "")
        if status == "✓":
            return "[green]✓[/green]"
        elif status == "✗":
            return "[red]✗[/red]"
        elif status == "⏳":
            return "[yellow]⠴[/yellow]"  # Use spinner icon while processing
        elif status == "→":
            return "[cyan]→[/cyan]"
        elif status == "⏸":
            return "[dim]⏸[/dim]"
        elif status == "⊘":
            return "[dim]⊘[/dim]"  # Skipped status
        return " "  # Return space to maintain width


class NameColumn(TextColumn):
    """Column to show task name."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render task name."""
        return str(task.fields.get("name", ""))


class MaterialisationColumn(TextColumn):
    """Column to show materialisation type."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render materialisation info."""
        materialise = task.fields.get("materialise", "")
        return f"[dim]{materialise}[/dim]"


class StrategyColumn(TextColumn):
    """Column to show strategy (if any)."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render strategy info."""
        strategy = task.fields.get("strategy", "")
        if strategy:
            # Use dim style like MaterialisationColumn for consistency
            return f"[dim]{strategy}[/dim]"
        return ""


class StepColumn(TextColumn):
    """Column to show current execution step."""

    # Fixed width based on longest possible step text
    # Possible values: "pending", "waiting", "ready", "executing", "materialising", "completed", "failed", "skipped"
    # Longest is "materialising" (13 chars)
    STEP_WIDTH = 13

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render current step info with fixed width."""
        step = task.fields.get("step", "")
        if step:
            # Use dim style for unobtrusive display
            # Pad to fixed width for consistent column width
            return f"[dim]{step:<{self.STEP_WIDTH}}[/dim]"
        return " " * self.STEP_WIDTH  # Return spaces to maintain width


class RowsColumn(TextColumn):
    """Column to show rows processed."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("", justify="right")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render rows count."""
        rows = task.fields.get("rows")
        if rows is not None:
            return f"[cyan]{rows}[/cyan]"
        elif task.fields.get("rows_ingested") is not None:
            return f"[cyan]{task.fields.get('rows_ingested')}[/cyan]"
        return ""


class ArrowColumn(TextColumn):
    """Column to show arrow separator when there are changes."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render arrow if there are changes."""
        # Show arrow only if there are any changes
        has_changes = (
            (task.fields.get("rows_inserted") is not None and task.fields.get("rows_inserted", 0) > 0)
            or (task.fields.get("rows_updated") is not None and task.fields.get("rows_updated", 0) > 0)
            or (task.fields.get("rows_deleted") is not None and task.fields.get("rows_deleted", 0) > 0)
        )
        if has_changes:
            return "[dim]⇒[/dim]"
        return ""


class InsertedColumn(TextColumn):
    """Column to show inserted rows count."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("", justify="right")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render inserted rows count."""
        rows_inserted = task.fields.get("rows_inserted")
        if rows_inserted is not None and rows_inserted > 0:
            return f"[green]+{rows_inserted}[/green]"
        return ""


class UpdatedColumn(TextColumn):
    """Column to show updated rows count."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("", justify="right")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render updated rows count."""
        rows_updated = task.fields.get("rows_updated")
        if rows_updated is not None and rows_updated > 0:
            return f"[yellow]~{rows_updated}[/yellow]"
        return ""


class DeletedColumn(TextColumn):
    """Column to show deleted rows count."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("", justify="right")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render deleted rows count."""
        rows_deleted = task.fields.get("rows_deleted")
        if rows_deleted is not None and rows_deleted > 0:
            return f"[red]-{rows_deleted}[/red]"
        return ""


class DependenciesColumn(TextColumn):
    """Column to show dependencies."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render dependencies."""
        deps = task.fields.get("dependencies", [])
        if deps:
            return f"[dim]{', '.join(deps)}[/dim]"
        return ""  # Empty if no deps


class SchemaChangesColumn(TextColumn):
    """Column to show schema changes."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render schema changes."""
        changes = task.fields.get("schema_changes", 0)
        if changes > 0:
            return f"[yellow]+{changes} schema[/yellow]"
        return ""


class ExecutionTimeColumn(TextColumn):
    """Column to show execution time in seconds."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render execution time if available."""
        elapsed = task.fields.get("execution_time")
        if elapsed is not None:
            return f"{elapsed:.2f}s"
        return ""


class FlowTimeColumn(TextColumn):
    """Column to show flow execution time in seconds."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render flow time if available."""
        time_str = task.fields.get("time", "")
        return str(time_str)


class ErrorColumn(TextColumn):
    """Column to show error messages (first line only)."""

    def __init__(self) -> None:
        """Initialize with empty text_format, we'll override render()."""
        super().__init__("")

    def render(self, task: Any) -> RenderableType:  # type: ignore[override]
        """Render error message first line if available."""
        error_msg = task.fields.get("error_message")
        if error_msg:
            # Get first line only
            first_line = error_msg.split("\n")[0]
            # Truncate if too long
            if len(first_line) > 60:
                first_line = first_line[:57] + "..."
            return f"[red]{first_line}[/red]"
        return ""


class Display:
    """
    Manages display for Interlace execution.

    Organizes output into sections:
    - Header at top (project info)
    - Logs in middle (takes remaining space)
    - Footer at bottom (flow and task progress)

    This is a singleton - use get_display() to get the global instance.
    """

    _instance: Optional["Display"] = None
    _lock = threading.Lock()

    def __init__(self, console: Console | None = None, enabled: bool = True, max_task_height: int = 20):
        """
        Initialize display manager.

        Args:
            console: Rich Console instance (creates new if None)
            enabled: Whether to enable display (default: True)
            max_task_height: Maximum number of task rows to display (default: 20)
        """
        self.enabled = enabled
        self.console = console if console is not None else Console()
        self.progress: Progress | None = None
        self.progress_tasks: dict[str, TaskID] = {}  # model_name -> task_id
        self.flow: Flow | None = None
        self._errors: dict[str, str] = {}  # model_name -> error_message
        self._update_lock = threading.Lock()
        self.flow_progress: Progress | None = None
        self.flow_progress_task_id: TaskID | None = None
        self.max_task_height = max_task_height
        self.config: dict[str, Any] | None = None
        self.state_store: Any | None = None
        self.models: dict[str, dict[str, Any]] | None = None
        self._project_name: str | None = None  # Store project name for header
        self._current_env: str | None = None  # Store current environment
        self._project_dir: Path | None = None  # Store project directory for finding env configs
        self.thread_pool_size: int | None = None  # Thread pool size for determining visible tasks
        self.task_visibility: dict[str, bool] = {}  # Track which tasks are visible
        self.live: Live | None = None

    @classmethod
    def get_instance(cls) -> "Display":
        """Get the global Display instance (creates if doesn't exist)."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _get_last_flow_info(self) -> dict[str, Any]:
        """Get last flow run information from state database."""
        if not self.state_store:
            return {}

        try:
            conn = self.state_store._get_connection()
            if conn is None:
                return {}

            # Query last completed flow using ibis
            import ibis

            try:
                flows_table = conn.table("interlace.flows")

                # Filter for completed/failed flows, order by completed_at desc, limit 1
                last_flow = (
                    flows_table.filter(flows_table.status.isin(["completed", "failed"]))
                    .order_by(ibis.desc(flows_table.completed_at))
                    .limit(1)
                )

                result = last_flow.execute()

                if not result.empty:
                    row = result.iloc[0]
                    return {
                        "flow_id": row.get("flow_id"),
                        "started_at": row.get("started_at"),
                        "completed_at": row.get("completed_at"),
                        "duration": row.get("duration_seconds"),
                        "status": row.get("status"),
                    }
            except Exception as e:
                # Table might not exist yet - this is expected on first run
                _display_logger.debug(f"Could not query flows table (may not exist yet): {e}")
        except Exception as e:
            _display_logger.debug(f"Could not get last flow info: {e}")

        return {}

    def _get_connection_info(self) -> list[dict[str, Any]]:
        """Get connection information (may be a list)."""
        if not self.config:
            return []

        try:
            connections = self.config.get("connections", {})
            if not connections:
                return []

            # If connections is a list, return it directly
            if isinstance(connections, list):
                return connections

            # If connections is a dict, convert to list
            conn_list = []
            for conn_name, conn_config in connections.items():
                if isinstance(conn_config, dict):
                    conn_type = conn_config.get("type", "unknown")
                    conn_list.append(
                        {
                            "name": conn_name,
                            "type": conn_type,
                        }
                    )
                else:
                    # If it's just a string or other type, use it as name
                    conn_list.append(
                        {
                            "name": conn_name,
                            "type": str(conn_config),
                        }
                    )

            return conn_list
        except Exception as e:
            _display_logger.debug(f"Could not parse connection info: {e}")
            return []

    def _get_available_environments(self) -> list[str]:
        """Get list of available environments from config files."""
        if not self._project_dir:
            return []

        try:
            from pathlib import Path

            project_path = Path(self._project_dir)
            env_files = list(project_path.glob("config.*.yaml"))
            environments = []
            for env_file in env_files:
                # Extract environment name from "config.{env}.yaml"
                env_name = env_file.stem.replace("config.", "")
                if env_name and env_name != "yaml":  # Skip if malformed
                    environments.append(env_name)
            return sorted(environments)
        except Exception as e:
            _display_logger.debug(f"Could not list environment files: {e}")
            return []

    def print_heading(
        self,
        project_name: str = "Interlace Project",
        config: dict[str, Any] | None = None,
        state_store: Any | None = None,
        models: dict[str, dict[str, Any]] | None = None,
        env: str | None = None,
        project_dir: Path | None = None,
    ) -> None:
        """
        Print project heading with key info - will appear above progress bars.

        Args:
            project_name: Name of the project
            config: Configuration dictionary
            state_store: StateStore instance for querying last run
            models: Dictionary of model definitions
            env: Current environment name
            project_dir: Project directory path (for finding environment configs)
        """
        if not self.enabled:
            return

        self.config = config
        self.state_store = state_store
        self.models = models
        self._project_name = project_name  # Store for later use
        self._current_env = env  # Store current environment
        self._project_dir = project_dir  # Store project directory

        # Don't build header here - it will be built in _build_header_panel when display context is entered
        # This method just stores the config for later use

    def initialize_progress(
        self,
        flow: Flow,
        models: dict[str, dict[str, Any]],
        graph: Any,
        thread_pool_size: int | None = None,
    ) -> None:
        """
        Initialize progress display from Flow and models using Rich Progress.

        Args:
            flow: Flow object tracking execution
            models: Dictionary of model definitions
            graph: Dependency graph for ordering
            thread_pool_size: Number of threads (used to determine initial visible tasks)
        """
        if not self.enabled:
            return

        self.flow = flow
        self.models = models
        self.thread_pool_size = thread_pool_size

        # Create Rich Progress display for tasks with custom columns
        # Both Progress instances share the same console so logs appear above both
        # Column order: Status, Name, Materialisation, Strategy, Dependencies, SchemaChanges, ExecutionTime, Spinner, Step, Rows, Arrow, Inserted, Updated, Deleted, Error
        self.progress = Progress(
            StatusColumn(),
            NameColumn(),
            MaterialisationColumn(),
            StrategyColumn(),
            DependenciesColumn(),
            SchemaChangesColumn(),
            ExecutionTimeColumn(),
            SpinnerColumn(),  # Spinner after execution time
            StepColumn(),  # Step column (waiting, executing, materialising, etc.)
            RowsColumn(),  # Rows column (final count)
            ArrowColumn(),  # Arrow separator column
            InsertedColumn(),  # Inserted rows column
            UpdatedColumn(),  # Updated rows column
            DeletedColumn(),  # Deleted rows column
            ErrorColumn(),  # Error column at the end
            console=self.console,
            transient=False,
        )

        # Create overall flow progress (same width as tasks, including error column for alignment)
        # Match exact column structure for alignment
        # Use the same console as progress so logs appear above both
        self.flow_progress = Progress(
            TextColumn(""),  # Match StatusColumn (empty for flow)
            TextColumn("[progress.description]{task.description}"),  # Flow description (matches NameColumn position)
            TextColumn(""),  # Match MaterialisationColumn (empty for flow)
            TextColumn(""),  # Match StrategyColumn (empty for flow)
            TextColumn(""),  # Match DependenciesColumn (empty for flow)
            TextColumn(""),  # Match SchemaChangesColumn (empty for flow)
            FlowTimeColumn(),  # Time display (matches ExecutionTimeColumn position)
            BarColumn(),  # Progress bar (matches SpinnerColumn position)
            TextColumn(""),  # Match StepColumn (empty for flow)
            TextColumn(""),  # Match RowsColumn (empty for flow)
            TextColumn(""),  # Match ArrowColumn (empty for flow)
            TextColumn(""),  # Match InsertedColumn (empty for flow)
            TextColumn(""),  # Match UpdatedColumn (empty for flow)
            TextColumn(""),  # Match DeletedColumn (empty for flow)
            TextColumn(""),  # Match ErrorColumn (empty for flow) - included for width alignment
            console=self.progress.console,  # Use progress console so logs appear above both
            transient=False,
        )

        # Initialize flow progress task
        total_tasks = len(flow.tasks)
        self.flow_progress_task_id = self.flow_progress.add_task(
            description="[bold]Flow Progress[/bold]",
            total=total_tasks,
            time="",  # Initialize time field
        )

        # Get layer mapping from graph to order progress bars by execution layer
        model_layers = graph.get_layers()

        # Sort models by layer for progress display
        sorted_models = sorted(models.keys(), key=lambda m: (model_layers.get(m, 999), m))

        # Calculate initial number of visible tasks: min(models, threads)
        num_models = len(sorted_models)
        if self.thread_pool_size is not None:
            initial_visible = min(num_models, self.thread_pool_size)
        else:
            # Fallback to max_task_height if thread_pool_size not provided
            initial_visible = min(num_models, self.max_task_height)

        # Initialize progress tasks for ALL models (we'll control visibility dynamically)
        for model_name in sorted_models:
            task = flow.tasks.get(model_name)
            if not task:
                continue

            # Create Rich progress task with all fields
            progress_task_id = self.progress.add_task(
                description="",  # Empty description, using columns instead
                total=1,
                visible=(sorted_models.index(model_name) < initial_visible),  # Initially visible based on position
            )
            self.progress_tasks[model_name] = progress_task_id
            self.task_visibility[model_name] = sorted_models.index(model_name) < initial_visible

            # Set initial field values
            self._update_progress_task(progress_task_id, task)

    def set_flow(self, flow: Flow) -> None:
        """
        Set the current Flow to observe.

        Args:
            flow: Flow object to observe
        """
        self.flow = flow
        # Auto-update when flow is set
        if self.enabled:
            self.update_from_flow()

    def update_from_flow(self) -> None:
        """Update progress display from Flow/Task objects."""
        if not self.enabled or not self.progress or not self.flow:
            return

        # CRITICAL: Use non-blocking lock acquisition to avoid deadlocks.
        # If the lock is held (e.g., by logging handler accessing display state),
        # skip this update. The next update will catch up.
        # This prevents deadlocks when logging happens during display updates.
        lock_acquired = self._update_lock.acquire(blocking=False)
        if not lock_acquired:
            # Lock is held - skip this update to avoid deadlock
            # This can happen if logging handler is accessing display state
            return

        try:
            # Update overall flow progress
            if self.flow_progress and self.flow_progress_task_id is not None:
                summary = self.flow.get_summary()
                completed = summary.get("completed", 0)
                total = summary.get("total_tasks", 0)
                failed = summary.get("failed", 0)
                skipped = summary.get("skipped", 0)
                status_text = f"{completed}/{total} completed"
                if failed > 0:
                    status_text += f", {failed} failed"
                if skipped > 0:
                    status_text += f", {skipped} skipped"

                # Get flow duration for time display
                flow_duration = self.flow.get_duration()
                time_str = f"{flow_duration:.2f}s" if flow_duration is not None else ""

                # Include skipped in completed count for progress bar
                completed_with_skipped = completed + skipped
                self.flow_progress.update(
                    self.flow_progress_task_id,
                    completed=completed_with_skipped,
                    description=f"[bold]Flow Progress: {status_text}[/bold]",
                    time=time_str,  # Set time field for display
                )

            # Update individual task progress with dynamic visibility
            self._update_task_visibility()

            # Update all tasks (visibility is handled in _update_task_visibility)
            for model_name, task in self.flow.tasks.items():
                if model_name not in self.progress_tasks:
                    continue

                task_id = self.progress_tasks[model_name]
                self._update_progress_task(task_id, task)
        finally:
            # Always release the lock, even if an exception occurs
            self._update_lock.release()

    def _update_task_visibility(self) -> None:
        """
        Dynamically manage task visibility:
        - When flow is complete: show all tasks
        - During execution:
          - If models < threads: show all tasks
          - If models > threads:
            1. Always show executing tasks (RUNNING, MATERIALISING)
            2. Then show pending tasks (PENDING, WAITING, READY) up to limit
            3. Then show completed tasks (COMPLETED, FAILED, SKIPPED) up to limit
        """
        if not self.progress or not self.flow:
            return

        # Check if flow is complete - if so, show ALL tasks regardless of status/models/threads
        if self.flow.status in [FlowStatus.COMPLETED, FlowStatus.FAILED, FlowStatus.CANCELLED]:
            # Flow is complete: make ALL tasks visible (no limits, no filtering)
            for model_name in self.progress_tasks.keys():
                self.task_visibility[model_name] = True
            # Update all Rich progress tasks to be visible
            for _model_name, task_id in self.progress_tasks.items():
                try:
                    self.progress.update(task_id, visible=True)
                except KeyError:
                    # Task doesn't exist in progress tracker - skip
                    pass
            return

        # Determine max visible based on thread pool size or max_task_height
        num_models = len(self.flow.tasks)
        if self.thread_pool_size is not None:
            max_visible = min(num_models, self.thread_pool_size)
        else:
            max_visible = min(num_models, self.max_task_height)

        # If models < threads, show all tasks
        if num_models <= max_visible:
            for model_name in self.progress_tasks.keys():
                self.task_visibility[model_name] = True
            # Update Rich progress tasks
            for _model_name, task_id in self.progress_tasks.items():
                try:
                    self.progress.update(task_id, visible=True)
                except KeyError:
                    # Task doesn't exist in progress tracker - skip
                    pass
            return

        # Models > threads: prioritize by status
        # Categorize all tasks by status
        executing_tasks = []  # RUNNING, MATERIALISING
        pending_tasks = []  # PENDING, WAITING, READY
        completed_tasks = []  # COMPLETED, FAILED, SKIPPED

        for model_name, task in self.flow.tasks.items():
            if model_name not in self.progress_tasks:
                continue
            if task.status in [TaskStatus.RUNNING, TaskStatus.MATERIALISING]:
                executing_tasks.append(model_name)
            elif task.status in [TaskStatus.PENDING, TaskStatus.WAITING, TaskStatus.READY]:
                pending_tasks.append(model_name)
            elif task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED]:
                completed_tasks.append(model_name)

        # Reset all visibility first
        for model_name in self.progress_tasks.keys():
            self.task_visibility[model_name] = False

        visible_count = 0

        # Step 1: Always show executing tasks (they take priority)
        for model_name in executing_tasks:
            self.task_visibility[model_name] = True
            visible_count += 1

        # Step 2: Show pending tasks up to limit
        for model_name in pending_tasks:
            if visible_count >= max_visible:
                break
            self.task_visibility[model_name] = True
            visible_count += 1

        # Step 3: Show completed tasks up to limit
        for model_name in completed_tasks:
            if visible_count >= max_visible:
                break
            self.task_visibility[model_name] = True
            visible_count += 1

        # Step 4: Update Rich progress tasks with visibility
        for model_name, task_id in self.progress_tasks.items():
            visible = self.task_visibility.get(model_name, False)
            # Update visibility directly - Rich will handle the actual visibility change
            # We don't need to check current state since update() is idempotent
            try:
                self.progress.update(task_id, visible=visible)
            except KeyError:
                # Task doesn't exist in progress tracker - skip
                pass

    def _update_progress_task(self, task_id: TaskID, task: Task) -> None:
        """Update a single progress task from Task object with all fields."""
        if not self.progress:
            return

        # Get current visibility state
        visible = self.task_visibility.get(task.model_name, True)

        # Set all fields for the task
        status_emoji = self._get_status_emoji(task.status)

        # Get error message (first line only for error column)
        error_msg = None
        if task.status == TaskStatus.FAILED:
            error_msg = self._errors.get(task.model_name, task.error_message or "Error")

        # Update progress task with all fields
        elapsed = task.get_duration()
        # Ensure strategy is set correctly - preserve actual strategy value, use empty string only if None
        strategy_value = task.strategy if task.strategy is not None else ""
        # Get step text from status
        step_text = self._get_step_text(task.status)
        self.progress.update(
            task_id,
            description="",  # Empty, using columns
            completed=(1 if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED] else 0),
            visible=visible,  # Set visibility
            # Set all custom fields
            status=status_emoji,
            name=task.model_name,
            materialise=task.materialise,
            strategy=strategy_value,  # Set strategy field for StrategyColumn to read
            dependencies=task.dependencies or [],
            schema_changes=task.schema_changes or 0,
            execution_time=elapsed,
            step=step_text,  # Set step field for StepColumn to read
            rows=task.rows_processed,
            rows_ingested=task.rows_ingested,
            rows_inserted=task.rows_inserted,
            rows_updated=task.rows_updated,
            rows_deleted=task.rows_deleted,
            error_message=error_msg,
        )

    def _get_status_emoji(self, status: TaskStatus) -> str:
        """Get emoji for task status."""
        status_map = {
            TaskStatus.PENDING: "⏸",
            TaskStatus.WAITING: "⏸",
            TaskStatus.READY: "→",
            TaskStatus.RUNNING: "⏳",
            TaskStatus.MATERIALISING: "⏳",
            TaskStatus.COMPLETED: "✓",
            TaskStatus.FAILED: "✗",
            TaskStatus.SKIPPED: "⊘",
        }
        return status_map.get(status, "?")

    def _get_step_text(self, status: TaskStatus) -> str:
        """Get human-readable step text from task status (lowercase for step column)."""
        step_map = {
            TaskStatus.PENDING: "pending",
            TaskStatus.WAITING: "waiting",
            TaskStatus.READY: "ready",
            TaskStatus.RUNNING: "executing",
            TaskStatus.MATERIALISING: "materialising",
            TaskStatus.COMPLETED: "completed",
            TaskStatus.FAILED: "failed",
            TaskStatus.SKIPPED: "skipped",
        }
        return step_map.get(status, "")

    def add_error(self, model_name: str, error_message: str) -> None:
        """
        Add error message for a model.

        Args:
            model_name: Name of the model
            error_message: Error message
        """
        with self._update_lock:
            self._errors[model_name] = error_message
            if self.flow and model_name in self.flow.tasks:
                task = self.flow.tasks[model_name]
                task.error_message = error_message
                # Update display if progress is active
                if self.progress:
                    self.update_from_flow()

    def print_errors(self) -> None:
        """Print error section to Progress console with syntax highlighting."""
        if not self.enabled or not self._errors:
            return

        # Use progress console if available, otherwise main console
        console_to_use = self.progress.console if self.progress else self.console

        # Build error content using Group to properly render Rich markup

        error_components: list[RenderableType] = []

        for model_name, error_msg in self._errors.items():
            # Create model name header with proper Rich markup
            model_header = Text.from_markup(f"[bold red]✗ {model_name}[/bold red]")
            error_components.append(model_header)

            # Use syntax highlighting for error messages
            error_lines = error_msg.split("\n")[:10]  # First 10 lines
            error_preview = "\n".join(error_lines)
            if len(error_msg.split("\n")) > 10:
                error_preview += "\n  ..."

            # Add syntax-highlighted error
            try:
                syntax = Syntax(error_preview, "python", theme="monokai", line_numbers=False, word_wrap=True)
                error_components.append(syntax)
            except Exception:
                # Fallback to plain text with indentation
                error_components.append(Text(f"  {error_preview}", style="dim"))

            # Add spacing between errors
            error_components.append(Text(""))

        if error_components:
            # Use Group to properly render all components
            error_group = Group(*error_components)
            panel = Panel(
                error_group,
                title="[bold red]Errors[/bold red]",
                border_style="red",
                padding=(1, 2),
            )
            console_to_use.print(panel)

    def __enter__(self) -> "Display":
        """Context manager entry - start Progress display."""
        if not self.enabled:
            return self

        # Combine flow progress and task progress into a Group
        # Use Live to display the group (which contains both Progress instances)
        if self.progress and self.flow_progress:
            progress_group = Group(self.flow_progress, self.progress)
            # Use Live to display the group - this allows logs to appear above progress
            self.live = Live(progress_group, console=self.console, refresh_per_second=10, screen=False)
            self.live.__enter__()

            # Initialize LogHandler's cached console NOW to avoid accessing display state during logging
            # This prevents potential deadlocks when errors occur
            try:
                from interlace.utils.logging import get_logger

                logger = get_logger("interlace")
                for handler in logger.handlers:
                    if hasattr(handler, "_cached_console") and handler._cached_console is None:
                        # Set the cached console to the progress console
                        handler._cached_console = self.progress.console
            except Exception as e:
                # If we can't update LogHandler, continue anyway - display works without this optimization
                _display_logger.debug(f"Could not initialize LogHandler cached console: {e}")

            # Print header to progress console AFTER entering context
            # This ensures header appears first in the progress display, before any logs
            if self._project_name:
                header_panel = self._build_header_panel()
                if header_panel:
                    self.progress.console.print(header_panel)
        elif self.progress:
            # Just use progress if flow_progress not available
            self.progress.__enter__()

            # Initialize LogHandler's cached console NOW to avoid accessing display state during logging
            try:
                from interlace.utils.logging import get_logger

                logger = get_logger("interlace")
                for handler in logger.handlers:
                    if hasattr(handler, "_cached_console") and handler._cached_console is None:
                        # Set the cached console to the progress console
                        handler._cached_console = self.progress.console
            except Exception as e:
                # If we can't update LogHandler, continue anyway - display works without this optimization
                _display_logger.debug(f"Could not initialize LogHandler cached console: {e}")

            # Print header to progress console
            if self._project_name:
                header_panel = self._build_header_panel()
                if header_panel:
                    self.progress.console.print(header_panel)

        return self

    def _build_header_panel(self) -> Panel | None:
        """Build header panel (internal method for reuse)."""
        if not self.enabled or not self._project_name:
            return None

        # Get header info
        num_models = len(self.models) if self.models else 0
        last_flow = self._get_last_flow_info()
        conn_list = self._get_connection_info()
        available_envs = self._get_available_environments()

        # Build header content - project name in bold blue, centered
        # Create project name text
        project_name_text = Text.from_markup(f"[bold blue]{self._project_name}[/bold blue]")

        # Get description from config (muted style)
        description = None
        if self.config:
            # Config can be either a dict or Config object
            if isinstance(self.config, dict):
                description = self.config.get("description")
            elif hasattr(self.config, "get"):
                description = self.config.get("description")
            elif hasattr(self.config, "data") and isinstance(self.config.data, dict):
                description = self.config.data.get("description")
            else:
                description = getattr(self.config, "description", None)

        # Key info line - build as Rich Text to support markup
        info_parts_text = []
        info_parts_text.append(Text(f"{num_models} models", style="dim"))

        # Add environments (highlight current)
        if available_envs:
            env_text = Text("Env: ", style="dim")
            for i, env in enumerate(available_envs):
                if i > 0:
                    env_text.append(" | ", style="dim")
                if env == self._current_env:
                    # Highlight current environment
                    env_text.append(env, style="bold cyan")
                else:
                    env_text.append(env, style="dim")
            info_parts_text.append(env_text)

        if last_flow:
            completed_at = last_flow.get("completed_at")
            duration = last_flow.get("duration")
            if completed_at:
                if isinstance(completed_at, str):
                    try:
                        dt = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
                        last_run_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        last_run_str = str(completed_at)
                else:
                    last_run_str = str(completed_at)
                info_parts_text.append(Text(f"Last run: {last_run_str}", style="dim"))

            if duration is not None:
                info_parts_text.append(Text(f"Duration: {duration:.2f}s", style="dim"))

        # Add connections (handle as list)
        if conn_list:
            if len(conn_list) == 1:
                conn = conn_list[0]
                conn_name = conn.get("name", "")
                conn_type = conn.get("type", "")
                info_parts_text.append(Text(f"Connection: {conn_name} ({conn_type})", style="dim"))
            else:
                # Multiple connections - show as list
                conn_names = [f"{c.get('name', '')} ({c.get('type', '')})" for c in conn_list]
                info_parts_text.append(Text(f"Connections: {', '.join(conn_names)}", style="dim"))

        # Build header content - center each line

        # Build lines separately for centering
        lines = []

        # Project name (centered)
        lines.append(Align.center(project_name_text))

        # Description (centered, if present)
        if description:
            description_text = Text(str(description), style="dim")
            lines.append(Align.center(description_text))

        # Info line (centered, if present)
        if info_parts_text:
            # Join info parts with " | " separator
            info_line = Text()
            for i, part in enumerate(info_parts_text):
                if i > 0:
                    info_line.append(" | ", style="dim")
                info_line.append(part)
            lines.append(Align.center(info_line))

        # Combine all centered lines
        header_content = Group(*lines) if len(lines) > 1 else lines[0] if lines else Text()

        # Create panel
        return Panel(
            header_content,
            title="[bold]Interlace[/bold]",
            title_align="left",
            border_style="blue",
            padding=(0, 1),
        )

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        """Context manager exit - stop Progress display and show errors."""
        if self.enabled:
            # For KeyboardInterrupt, exit immediately without printing errors
            # (errors will be shown in the progress display anyway)
            if exc_type is KeyboardInterrupt:
                # Exit progress context immediately
                if self.live:
                    try:
                        self.live.__exit__(exc_type, exc_val, exc_tb)
                    except Exception as e:
                        _display_logger.debug(f"Error exiting live display on interrupt: {e}")
                    self.live = None
                elif self.progress:
                    try:
                        self.progress.__exit__(exc_type, exc_val, exc_tb)
                    except Exception as e:
                        _display_logger.debug(f"Error exiting progress display on interrupt: {e}")
                # Don't suppress KeyboardInterrupt - let it propagate
                return

            # Print errors before closing (for non-interrupt exits)
            self.print_errors()

            # Exit progress context
            if self.live:
                self.live.__exit__(exc_type, exc_val, exc_tb)
                self.live = None
            elif self.progress:
                self.progress.__exit__(exc_type, exc_val, exc_tb)


# Global instance accessor
def get_display() -> Display:
    """
    Get the global Display instance.

    Returns:
        Display: The global display instance
    """
    return Display.get_instance()
