"""
Logging configuration for Interlace.

Phase 0: Setup logging infrastructure with console and optional file output.
"""

import logging
import sys
import threading
from pathlib import Path
from typing import Any

try:
    import importlib.util

    RICH_AVAILABLE = (
        importlib.util.find_spec("rich.console") is not None and importlib.util.find_spec("rich.logging") is not None
    )
except Exception:
    RICH_AVAILABLE = False


class FileFormatter(logging.Formatter):
    """Formatter for file logs - clean and parseable."""

    def __init__(self) -> None:
        super().__init__(fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with full exception info for errors."""
        # Base format
        result = super().format(record)

        # Add exception info if available
        if record.exc_info:
            import traceback

            result += "\n" + "".join(traceback.format_exception(*record.exc_info))

        return result


# Map string level names to logging constants
LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def _parse_level(level: str | int) -> int:
    """
    Parse logging level from string or int.

    Args:
        level: Logging level as string (DEBUG, INFO, etc.) or int

    Returns:
        Logging level constant
    """
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        level_upper = level.upper()
        if level_upper in LEVEL_MAP:
            return LEVEL_MAP[level_upper]
    # Default to INFO if invalid
    return logging.INFO


def setup_logging(
    level: str | int = logging.INFO,
    log_file: str | Path | None = None,
    format_string: str | None = None,
    file_mode: str = "a",
    console: Any | None = None,
    console_enabled: bool = True,
    use_rich: bool = True,
) -> logging.Logger:
    """
    Setup logging configuration for Interlace.

    Args:
        level: Logging level as string (DEBUG, INFO, etc.) or int (default: INFO)
        log_file: Optional file path to write logs to (default: None, console only)
        format_string: Optional custom format string (default: standard format)
        file_mode: File mode for file handler - 'a' for append, 'w' for overwrite (default: 'a')
        console: Optional Rich Console instance to use (default: None, creates new)
        console_enabled: Whether to enable console logging (default: True)
        use_rich: Whether to use RichHandler for better integration with Rich progress bars (default: True)

    Returns:
        Logger instance
    """
    logger = logging.getLogger("interlace")

    # Remove existing handlers to avoid duplicates
    # Only clear handlers from this specific logger, not root or child loggers
    logger.handlers.clear()

    # Also clear any file handlers from root logger that might have been added previously
    # to prevent duplicates
    root_logger = logging.getLogger()
    root_file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
    for fh in root_file_handlers:
        root_logger.removeHandler(fh)

    # Parse level
    level_int = _parse_level(level)

    # Set level on interlace logger
    # Child loggers will inherit this level if not explicitly set
    logger.setLevel(level_int)

    # Also set root logger level to ensure proper inheritance chain
    # But don't set it too low to avoid noise from other libraries
    root_logger = logging.getLogger()
    if root_logger.level == logging.NOTSET or root_logger.level > level_int:
        root_logger.setLevel(level_int)

    # Console handler (if enabled)
    if console_enabled:
        if use_rich and RICH_AVAILABLE:
            # Use LogHandler for Rich display (handles Layout integration)
            from interlace.utils.display import LogHandler

            logger.addHandler(
                LogHandler(
                    level=level_int,
                    show_time=True,
                    show_path=True,
                    markup=True,
                    rich_tracebacks=True,
                    tracebacks_show_locals=False,
                    show_level=True,
                    log_time_format="[%X]",  # [HH:MM:SS] format (no date)
                    omit_repeated_times=False,  # Show timestamps for all logs
                )
            )
        else:
            # Standard handler (Rich not available or not requested)
            # Create formatter
            if format_string is None:
                # Custom format: "level: timestamp - msg" for normal logs
                # For errors, include class/file information
                class CustomFormatter(logging.Formatter):
                    def __init__(self) -> None:
                        super().__init__(datefmt="%Y-%m-%d %H:%M:%S")

                    def format(self, record: logging.LogRecord) -> str:
                        # Base format: "level: timestamp - msg"
                        base_format = f"{record.levelname}: {self.formatTime(record)} - {record.getMessage()}"

                        # For errors, add class/file info
                        if record.levelno >= logging.ERROR:
                            if hasattr(record, "pathname") and record.pathname:
                                # Extract filename from path
                                filename = Path(record.pathname).name
                                if hasattr(record, "lineno") and record.lineno:
                                    base_format = f"{record.levelname}: {self.formatTime(record)} - {filename}:{record.lineno} - {record.getMessage()}"
                                else:
                                    base_format = f"{record.levelname}: {self.formatTime(record)} - {filename} - {record.getMessage()}"

                        return base_format

                formatter: logging.Formatter = CustomFormatter()
            else:
                formatter = logging.Formatter(format_string)

            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.setLevel(level_int)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        log_file = Path(log_file)
        # Create parent directory if it doesn't exist
        log_file.parent.mkdir(parents=True, exist_ok=True)

        # Check if a file handler for this file already exists to avoid duplicates
        existing_file_handler: logging.Handler | None = None
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler) and handler.baseFilename == str(log_file.resolve()):
                existing_file_handler = handler
                break

        if existing_file_handler is None:
            file_handler = logging.FileHandler(log_file, mode=file_mode)
            # Set file handler to DEBUG to capture all logs including errors
            # The logger level will still filter, but file should capture everything
            file_handler.setLevel(logging.DEBUG)
            # Use FileFormatter for clean, parseable output
            file_handler.setFormatter(FileFormatter())
            # Add file handler ONLY to interlace logger
            # Child loggers (like "interlace.executor") will propagate to parent
            logger.addHandler(file_handler)

        # Ensure child loggers propagate to parent (default, but be explicit)
        # This ensures errors from child loggers are captured by parent's file handler
        logger.propagate = True

    return logger


def setup_logging_from_config(
    config: dict[str, Any], project_dir: Path | None = None, console: Any | None = None
) -> logging.Logger:
    """
    Setup logging from Interlace configuration.

    Args:
        config: Configuration dictionary (can be nested under 'logging' key or flat)
        project_dir: Optional project directory for resolving relative log file paths
        console: Optional Rich Console instance for RichHandler

    Returns:
        Logger instance
    """
    # Get logging config (can be at root level or under 'logging' key)
    logging_config = config.get("logging", {})

    # If config itself is a dict and doesn't have 'logging' key, check if it has logging keys directly
    if not logging_config and isinstance(config, dict):
        # Check for direct logging keys
        if "level" in config or "log_file" in config or "format" in config:
            logging_config = {
                "level": config.get("level"),
                "file": config.get("log_file") or config.get("file"),
                "format": config.get("format"),
                "file_mode": config.get("file_mode", "a"),
            }

    # Extract values with defaults
    level = logging_config.get("level", logging.INFO)
    file_mode = logging_config.get("file_mode", "a")
    format_string = logging_config.get("format")

    # File logging configuration
    file_enabled = logging_config.get("file_enabled", True)  # Default: enabled
    log_file = None
    if file_enabled:
        log_file = logging_config.get("file") or logging_config.get("log_file")
        if log_file is None:
            # Default log file path
            log_file = "logs/interlace.log"

    # Console logging configuration
    console_enabled = logging_config.get("console_enabled", True)  # Default: enabled
    console_type = logging_config.get("console_type", "rich")  # Default: rich

    # Resolve log file path if relative and project_dir provided
    if log_file and project_dir:
        log_file = Path(log_file)
        if not log_file.is_absolute():
            log_file = project_dir / log_file

    # Setup logging
    use_rich = console_type == "rich" and console_enabled
    return setup_logging(
        level=level,
        log_file=log_file if file_enabled else None,
        format_string=format_string,
        file_mode=file_mode,
        console=console if use_rich else None,
        use_rich=use_rich and console_enabled,
    )


# Track if logging has been set up to avoid duplicate setup
_logging_setup_done = False
_logging_setup_lock = threading.Lock()
_stored_project_dir: Path | None = None


def _auto_setup_logging() -> None:
    """
    Automatically set up logging from global config if not already configured.

    This is called by get_logger() to ensure logging is configured even if
    setup_logging_from_config() wasn't called explicitly.
    """
    global _logging_setup_done, _stored_project_dir

    # Check if interlace logger already has handlers (already set up)
    interlace_logger = logging.getLogger("interlace")
    if interlace_logger.handlers:
        _logging_setup_done = True
        return

    # Only try to auto-setup once
    if _logging_setup_done:
        return

    with _logging_setup_lock:
        # Double-check pattern - another thread might have set it up
        if _logging_setup_done:
            return

        # Check again if handlers were added by another thread
        if interlace_logger.handlers:
            _logging_setup_done = True
            return

        try:
            from interlace.config.singleton import get_config

            config_obj = get_config()

            if config_obj is not None:
                # Get project_dir - try stored value first, then infer
                project_dir = _stored_project_dir
                if project_dir is None:
                    # Try to infer from config file location
                    from pathlib import Path

                    try:
                        # Check if we can find config.yaml in current or parent dirs
                        current = Path.cwd()
                        for _ in range(5):  # Check up to 5 levels up
                            if (current / "config.yaml").exists():
                                project_dir = current
                                break
                            current = current.parent
                    except Exception:
                        pass

                # Get display console if available
                console = None
                try:
                    from interlace.utils.display import get_display

                    display = get_display()
                    if display.enabled:
                        console = display.console
                except Exception:
                    pass

                # Setup logging from config
                setup_logging_from_config(config_obj.data, project_dir=project_dir, console=console)
                _logging_setup_done = True
        except Exception:
            # If auto-setup fails, continue without setup
            # This allows code to work even if config isn't available yet
            pass


def get_logger(name: str = "interlace") -> logging.Logger:
    """
    Get a logger instance.

    Automatically sets up logging from global config if not already configured.
    This allows models to use logging without explicit setup_logging_from_config calls.

    Args:
        name: Logger name (default: "interlace")

    Returns:
        Logger instance
    """
    # Auto-setup logging if not already done
    _auto_setup_logging()

    logger = logging.getLogger(name)
    # Ensure child loggers propagate to parent (default, but be explicit)
    # This ensures errors from child loggers (e.g., "interlace.executor") are captured
    # by parent logger handlers (like file handler)
    logger.propagate = True
    return logger
