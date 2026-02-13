"""
Structured logging for Interlace.

Phase 3: JSON-formatted logging with correlation IDs for distributed tracing.

Usage:
    from interlace.observability import setup_structured_logging, add_correlation_id

    # Enable structured logging
    setup_structured_logging(level="INFO", json_format=True)

    # Add correlation ID to logs
    with add_correlation_id("run_abc123"):
        logger.info("Processing model")  # Will include correlation_id in log
"""

import json
import logging
import sys
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

from interlace.utils.logging import get_logger

logger = get_logger("interlace.observability.logging")

# Context variable for correlation ID
_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def get_correlation_id() -> str | None:
    """
    Get current correlation ID.

    Returns:
        Correlation ID or None if not set
    """
    return _correlation_id.get()


def set_correlation_id(correlation_id: str) -> None:
    """
    Set correlation ID for current context.

    Args:
        correlation_id: Correlation ID to set
    """
    _correlation_id.set(correlation_id)


@contextmanager
def add_correlation_id(correlation_id: str | None = None) -> Any:
    """
    Context manager to add correlation ID to logs.

    Args:
        correlation_id: Correlation ID (auto-generated if not provided)

    Yields:
        The correlation ID

    Usage:
        with add_correlation_id("flow_abc123") as cid:
            logger.info("Processing")  # Logs will include correlation_id
    """
    cid = correlation_id or str(uuid.uuid4())[:8]
    token = _correlation_id.set(cid)
    try:
        yield cid
    finally:
        _correlation_id.reset(token)


class StructuredFormatter(logging.Formatter):
    """
    JSON-formatted log formatter with structured fields.

    Includes:
    - Timestamp (ISO 8601)
    - Level
    - Logger name
    - Message
    - Correlation ID (if set)
    - Exception info (if present)
    - Extra fields
    """

    def __init__(
        self,
        include_timestamp: bool = True,
        include_level: bool = True,
        include_logger: bool = True,
        include_correlation_id: bool = True,
        extra_fields: dict[str, Any] | None = None,
    ):
        """
        Initialize structured formatter.

        Args:
            include_timestamp: Include timestamp in output
            include_level: Include log level in output
            include_logger: Include logger name in output
            include_correlation_id: Include correlation ID if available
            extra_fields: Extra fields to include in all logs
        """
        super().__init__()
        self.include_timestamp = include_timestamp
        self.include_level = include_level
        self.include_logger = include_logger
        self.include_correlation_id = include_correlation_id
        self.extra_fields = extra_fields or {}

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: dict[str, Any] = {}

        # Add timestamp
        if self.include_timestamp:
            log_data["timestamp"] = datetime.now(UTC).isoformat()

        # Add level
        if self.include_level:
            log_data["level"] = record.levelname

        # Add logger name
        if self.include_logger:
            log_data["logger"] = record.name

        # Add message
        log_data["message"] = record.getMessage()

        # Add correlation ID
        if self.include_correlation_id:
            correlation_id = get_correlation_id()
            if correlation_id:
                log_data["correlation_id"] = correlation_id

        # Add location info
        log_data["location"] = {
            "file": record.filename,
            "line": record.lineno,
            "function": record.funcName,
        }

        # Add exception info
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info),
            }

        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in (
                "name",
                "msg",
                "args",
                "created",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "exc_info",
                "exc_text",
                "message",
                "thread",
                "threadName",
            ):
                # Skip internal attributes
                if not key.startswith("_"):
                    log_data[key] = value

        # Add static extra fields
        log_data.update(self.extra_fields)

        return json.dumps(log_data, default=str)


class HumanReadableFormatter(logging.Formatter):
    """
    Human-readable formatter with optional correlation ID.

    Format: [timestamp] [level] [logger] [correlation_id] message
    """

    def __init__(self, include_correlation_id: bool = True):
        """
        Initialize human-readable formatter.

        Args:
            include_correlation_id: Include correlation ID if available
        """
        super().__init__()
        self.include_correlation_id = include_correlation_id

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as human-readable string."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname.ljust(8)
        logger_name = record.name

        parts = [f"[{timestamp}]", f"[{level}]", f"[{logger_name}]"]

        # Add correlation ID
        if self.include_correlation_id:
            correlation_id = get_correlation_id()
            if correlation_id:
                parts.append(f"[{correlation_id}]")

        parts.append(record.getMessage())

        message = " ".join(parts)

        # Add exception info
        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)

        return message


def setup_structured_logging(
    level: str = "INFO",
    json_format: bool = False,
    stream: Any = None,
    extra_fields: dict[str, Any] | None = None,
) -> None:
    """
    Setup structured logging for Interlace.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        json_format: Use JSON format (True) or human-readable (False)
        stream: Output stream (defaults to sys.stderr)
        extra_fields: Extra fields to include in all logs (JSON format only)

    Usage:
        # Human-readable format
        setup_structured_logging(level="INFO")

        # JSON format for log aggregation
        setup_structured_logging(level="INFO", json_format=True)
    """
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create handler
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setLevel(getattr(logging, level.upper()))

    # Set formatter
    formatter: logging.Formatter
    if json_format:
        formatter = StructuredFormatter(extra_fields=extra_fields)
    else:
        formatter = HumanReadableFormatter()

    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    # Also configure interlace loggers
    interlace_logger = logging.getLogger("interlace")
    interlace_logger.setLevel(getattr(logging, level.upper()))

    logger.debug(f"Structured logging configured: level={level}, json={json_format}")


class LogContext:
    """
    Context manager for adding structured fields to logs.

    Usage:
        with LogContext(model="users", flow_id="abc123"):
            logger.info("Processing model")  # Will include model and flow_id
    """

    def __init__(self, **fields: Any) -> None:
        """
        Initialize log context.

        Args:
            **fields: Fields to add to log records
        """
        self.fields = fields
        self._old_factory: Any = None

    def __enter__(self) -> "LogContext":
        """Add fields to log records."""
        old_factory = logging.getLogRecordFactory()
        self._old_factory = old_factory
        fields = self.fields

        def record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
            record = old_factory(*args, **kwargs)
            for key, value in fields.items():
                setattr(record, key, value)
            return record

        logging.setLogRecordFactory(record_factory)
        return self

    def __exit__(self, *args: Any) -> None:
        """Restore original log record factory."""
        if self._old_factory:
            logging.setLogRecordFactory(self._old_factory)


def log_model_start(
    model_name: str,
    model_type: str = "python",
    materialise: str = "table",
    schema: str = "default",
) -> None:
    """
    Log model execution start.

    Args:
        model_name: Model name
        model_type: Model type (python, sql, stream)
        materialise: Materialization type
        schema: Schema name
    """
    log = logging.getLogger("interlace.execution")
    log.info(
        "Starting model execution",
        extra={
            "event": "model.start",
            "model": model_name,
            "model_type": model_type,
            "materialise": materialise,
            "schema": schema,
        },
    )


def log_model_end(
    model_name: str,
    success: bool,
    duration: float,
    rows_processed: int | None = None,
    error: str | None = None,
) -> None:
    """
    Log model execution end.

    Args:
        model_name: Model name
        success: Whether execution succeeded
        duration: Execution duration in seconds
        rows_processed: Number of rows processed
        error: Error message if failed
    """
    log = logging.getLogger("interlace.execution")

    extra = {
        "event": "model.end",
        "model": model_name,
        "success": success,
        "duration_seconds": duration,
    }

    if rows_processed is not None:
        extra["rows_processed"] = rows_processed

    if error:
        extra["error"] = error
        log.error("Model execution failed", extra=extra)
    else:
        log.info("Model execution completed", extra=extra)
