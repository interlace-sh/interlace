"""
Model decorator and model definition.

Phase 0: Unified @model decorator for Python and SQL models.
Phase 2: Added retry_policy for automatic retry on transient failures.
Phase 3: Added cache parameter for source caching with TTL.
Phase 4: Added schedule parameter for model-level scheduling.
Phase 5: Added schema_mode and export parameters.
Phase 6: Added cursor parameter for automatic incremental processing.
"""

import functools
import inspect
import re
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Optional

import ibis

from interlace.utils.logging import get_logger

if TYPE_CHECKING:
    from interlace.core.retry import RetryPolicy

logger = get_logger("interlace.model")

# Valid materialisation types accepted by the executor.
_VALID_MATERIALISE = {"table", "view", "ephemeral", "none"}

# Valid schema evolution modes.
_VALID_SCHEMA_MODES = {"strict", "safe", "flexible", "lenient", "ignore"}


def parse_ttl(ttl_str: str) -> float:
    """
    Parse a human-readable TTL string into seconds.

    Supported formats:
        - ``"30s"`` -- 30 seconds
        - ``"30m"`` -- 30 minutes
        - ``"24h"`` -- 24 hours
        - ``"7d"`` -- 7 days
        - ``"2w"`` -- 2 weeks

    Args:
        ttl_str: TTL string (e.g. ``"7d"``, ``"24h"``, ``"30m"``)

    Returns:
        TTL in seconds

    Raises:
        ValueError: If the TTL string format is invalid
    """
    if not isinstance(ttl_str, str):
        raise ValueError(
            f"TTL must be a string with a unit suffix (e.g. '30s', '24h', '7d'), "
            f"got {type(ttl_str).__name__}: {ttl_str}"
        )

    match = re.match(r"^(\d+(?:\.\d+)?)\s*([smhdw])$", ttl_str.strip().lower())
    if not match:
        raise ValueError(
            f"Invalid TTL format '{ttl_str}'. "
            f"Expected format: <number><unit> where unit is s/m/h/d/w "
            f"(e.g. '30s', '24h', '7d', '2w')"
        )

    value = float(match.group(1))
    unit = match.group(2)

    multipliers = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 604800,
    }

    return value * multipliers[unit]


def _format_duration(elapsed: float) -> str:
    """Format duration in human-readable format."""
    if elapsed < 1.0:
        return f"{elapsed*1000:.0f}ms"
    elif elapsed < 60.0:
        return f"{elapsed:.2f}s"
    else:
        minutes = int(elapsed // 60)
        seconds = elapsed % 60
        return f"{minutes}m{seconds:.1f}s"


def _make_wrapper(func: Callable, model_name: str, is_async: bool) -> Callable:
    """Create a timing/logging wrapper for a model function.

    Builds either an async or sync wrapper that logs execution start,
    success with duration, and failure with duration at DEBUG level.

    Args:
        func: The original model function to wrap.
        model_name: Human-readable model name for log messages.
        is_async: Whether *func* is a coroutine function.

    Returns:
        Wrapped function with ``functools.wraps`` applied.
    """

    if is_async:

        @functools.wraps(func)
        async def wrapper(*args: Any, **kw: Any) -> Any:
            start_time = time.time()
            try:
                logger.debug(f"Model '{model_name}' execution started")
                result = await func(*args, **kw)
                elapsed = time.time() - start_time
                logger.debug(f"Model '{model_name}' completed successfully " f"in {_format_duration(elapsed)}")
                return result
            except Exception as e:
                elapsed = time.time() - start_time
                # Log at DEBUG level to avoid duplication with executor-level
                # ERROR logging which provides full context and traceback.
                logger.debug(f"Model '{model_name}' failed after " f"{_format_duration(elapsed)}: {e}")
                raise

    else:

        @functools.wraps(func)
        def wrapper(*args: Any, **kw: Any) -> Any:
            start_time = time.time()
            try:
                logger.debug(f"Model '{model_name}' execution started")
                result = func(*args, **kw)
                elapsed = time.time() - start_time
                logger.debug(f"Model '{model_name}' completed successfully " f"in {_format_duration(elapsed)}")
                return result
            except Exception as e:
                elapsed = time.time() - start_time
                logger.debug(f"Model '{model_name}' failed after " f"{_format_duration(elapsed)}: {e}")
                raise

    return wrapper


def model(
    name: str | None = None,
    schema: str = "public",
    connection: str | None = None,
    materialise: str = "table",
    strategy: str | None = None,
    primary_key: str | list[str] | None = None,
    dependencies: list[str] | None = None,
    incremental: dict[str, Any] | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    owner: str | None = None,
    fields: dict[str, Any] | list[tuple[str, Any]] | ibis.Schema | None = None,
    strict: bool = False,
    column_mapping: dict[str, str] | None = None,
    retry_policy: Optional["RetryPolicy"] = None,
    cache: dict[str, str] | None = None,
    schedule: dict[str, str] | None = None,
    schema_mode: str | None = None,
    export: dict[str, Any] | None = None,
    cursor: str | None = None,
    **kwargs: Any,
) -> Callable[[Callable], Callable]:
    """
    Decorator to define an Interlace model.

    The decorator automatically wraps model functions with timing/success/failure logging
    at DEBUG level. This is completely transparent - users just use @model and get automatic
    logging. Executor-level logging (INFO/ERROR) provides full lifecycle context including
    materialization, row counts, and comprehensive error reporting with tracebacks.

    Model functions can be either async or sync:
    - Async functions: Use `async def` - await directly in executor
    - Sync functions: Use `def` - run in thread pool executor to avoid blocking

    Model functions can return:
    - pandas DataFrame
    - list of dicts: [{"col1": val1}, {"col1": val2}]
    - dict: {"col1": val1, "col2": val2} (single row)
    - None: For models that create tables as side effects (see BATCH_OPERATIONS.md)
    - generators: Functions using yield will have all yielded values collected
    - ibis.Table: Already converted tables

    All return types are automatically converted to ibis.Table internally.

    Schema Handling:
        Use `fields` to define expected columns and types. By default (strict=False),
        fields are merged with inferred schema - extra columns in data are kept.
        With strict=True, only the specified fields are output.

    Fields format supports native ibis schema formats:
    - Dict: {"x": int, "y": str} or {"x": "int64", "y": "string"}
    - List of tuples: [("foo", "string"), ("bar", "int64")]
    - Existing ibis.Schema object

    Source Caching:
        Use ``cache`` to control when source models re-execute. Useful for models
        that fetch from external APIs (GitHub, Companies House, etc.) where you
        don't want to re-fetch on every run.

        Cache strategies:
        - ``"ttl"`` (default): Skip execution if last run was within TTL
        - ``"if_exists"``: Skip if the materialised table exists (manual refresh only)
        - ``"always"``: Normal behaviour, no caching

        Example::

            @model(
                name="github_repos",
                cache={"ttl": "24h", "strategy": "ttl"},
                tags=["source"],
            )
            def github_repos():
                ...

    Scheduling:
        Use ``schedule`` to define when this model should be automatically executed
        by the background scheduler (when running ``interlace serve``).

        Supports cron expressions and interval-based scheduling.

        Example::

            @model(
                name="daily_report",
                schedule={"cron": "0 6 * * *", "timezone": "UTC"},
            )
            def daily_report(raw_data):
                ...

            @model(
                name="frequent_check",
                schedule={"every_s": "300"},  # Every 5 minutes
            )
            def frequent_check():
                ...

    Export:
        Use ``export`` to automatically export model output to a file after
        materialization. Supports CSV, Parquet, and JSON formats.

        Example::

            @model(
                name="report",
                export={"format": "csv", "path": "output/report.csv"},
            )
            def report(data):
                ...

    Cursor Tracking:
        Use ``cursor`` to enable automatic incremental processing for
        side-effect models (notifications, API calls, etc.). The executor
        filters each dependency to only include rows where the cursor column
        is greater than the last processed value, and saves the new watermark
        after successful execution.

        Works with any monotonically increasing column: integer IDs, timestamps,
        sequence numbers, etc.

        Example::

            @model(
                name="slack_alerts",
                materialise="none",
                cursor="event_id",
            )
            def slack_alerts(events):
                # Only receives events with event_id > last processed value
                for event in events.execute().to_dict(orient="records"):
                    send_slack(event)

    Args:
        name: Model name (defaults to function name)
        schema: Schema/database name (default: "public") - ibis uses "database" terminology
        connection: Connection name from config (defaults to "default" connection if exists, otherwise first connection)
        materialise: Materialisation type ("table", "view", "ephemeral", "none").
                     Use "none" for side-effect models that don't persist output.
        strategy: Data loading strategy ("merge_by_key", "append", "replace", "none")
        primary_key: Primary key column(s)
        dependencies: Explicit dependencies (auto-detected if None)
        incremental: Incremental configuration (key or datetime)
        description: Model description
        tags: Tags for organization
        owner: Owner/team name
        fields: Optional schema definition in native ibis format (dict, list of tuples, or Schema)
                Examples: {"id": int, "name": str} or [("id", "int64"), ("name", "string")]
        strict: If True, only output columns specified in fields (drop extra columns).
                If False (default), merge fields with inferred schema (keep extra columns).
        column_mapping: Simple dict for column renames: {"old_name": "new_name"}
        retry_policy: Optional retry policy for handling transient failures.
                     Example: retry_policy=RetryPolicy(max_attempts=5, initial_delay=2.0)
        cache: Optional cache policy for source models.
               Keys: ``ttl`` (e.g. "7d", "24h"), ``strategy`` ("ttl"|"if_exists"|"always").
               Example: cache={"ttl": "7d", "strategy": "ttl"}
        schedule: Optional schedule for automatic execution. Used by ``interlace serve``.
                  Keys: ``cron`` (5-field cron expression) OR ``every_s`` (interval in seconds),
                  ``timezone`` (optional, default UTC), ``max_concurrency`` (optional, default 1).
                  Example: schedule={"cron": "0 * * * *"} or schedule={"every_s": "600"}
        schema_mode: Schema evolution mode controlling how schema changes are handled.
                     One of: "strict", "safe" (default), "flexible", "lenient", "ignore".
        export: Optional export configuration for writing output to file.
                Keys: ``format`` ("csv"|"parquet"|"json"), ``path`` (output file path),
                plus format-specific options.
                Example: export={"format": "csv", "path": "output/report.csv"}
        cursor: Optional cursor column for automatic incremental processing.
                When set, the executor filters each dependency to rows where
                ``cursor_column > last_processed_value`` and updates the watermark
                after successful execution. Useful for side-effect models that
                send notifications, call APIs, etc.
                Example: cursor="event_id" or cursor="rowid"
        **kwargs: Additional model metadata

    Raises:
        ValueError: If ``materialise`` is not a recognised type.
        ValueError: If ``schema_mode`` is not a recognised mode.
    """

    # ------------------------------------------------------------------
    # Validate enum-like parameters eagerly so errors surface at import
    # time rather than deep inside the executor.
    # ------------------------------------------------------------------
    if materialise not in _VALID_MATERIALISE:
        raise ValueError(
            f"Invalid materialise type '{materialise}'. " f"Must be one of: {', '.join(sorted(_VALID_MATERIALISE))}"
        )

    if schema_mode is not None and schema_mode not in _VALID_SCHEMA_MODES:
        raise ValueError(
            f"Invalid schema_mode '{schema_mode}'. " f"Must be one of: {', '.join(sorted(_VALID_SCHEMA_MODES))}"
        )

    def decorator(func: Callable) -> Callable:
        model_name = name or func.__name__

        # Build metadata dict that the discovery/executor systems read.
        metadata = {
            "name": model_name,
            "schema": schema,
            "connection": connection,
            "materialise": materialise,
            "strategy": strategy,
            "primary_key": primary_key,
            "dependencies": dependencies,
            "incremental": incremental,
            "description": description or func.__doc__,
            "tags": tags or [],
            "owner": owner,
            "fields": fields,
            "strict": strict,
            "column_mapping": column_mapping,
            "retry_policy": retry_policy,
            "cache": cache,
            "schedule": schedule,
            "schema_mode": schema_mode,
            "export": export,
            "cursor": cursor,
            **kwargs,
        }

        # Create timing/logging wrapper (async or sync).
        wrapper = _make_wrapper(func, model_name, inspect.iscoroutinefunction(func))

        # Attach metadata to the wrapper explicitly so that model discovery
        # (which checks ``hasattr(obj, "_interlace_model")``) finds it
        # reliably, regardless of ``functools.wraps`` __dict__ propagation.
        wrapper._interlace_model = metadata  # type: ignore[attr-defined]

        return wrapper

    return decorator
