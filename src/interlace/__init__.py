"""
Interlace - A modern, Python/SQL-first data pipeline framework.

Phase 0 (MVP): Core functionality for basic data pipeline execution.
"""

__version__ = "0.1.2"

# Core exports
# Global config
from interlace.config.singleton import config

# Programmatic API
from interlace.core.api import run, run_sync
from interlace.core.context import get_connection, sql
from interlace.core.dependencies import build_dependency_graph
from interlace.core.executor import execute_models
from interlace.core.model import model
from interlace.core.stream import ack, consume, publish, publish_sync, stream, subscribe

# Exceptions
from interlace.exceptions import (
    CircuitBreakerOpenError,
    ConfigurationError,
    ConnectionLockError,
    ConnectionNotFoundError,
    DeadlockError,
    DependencyError,
    ExecutionError,
    InitializationError,
    InterlaceConnectionError,
    InterlaceError,
    MaterializationError,
    ModelExecutionError,
    QualityError,
    RetryError,
    SchemaError,
    StateStoreError,
)

# Testing utilities
from interlace.testing import TestResult, mock_dependency, test_model, test_model_sync

# API utilities
from interlace.utils.api import API, basic_auth_token, oauth2_token

# Logging utilities
from interlace.utils.logging import get_logger, setup_logging, setup_logging_from_config

__all__ = [
    # Core decorators
    "model",
    "stream",
    # Stream API
    "publish",
    "publish_sync",
    "subscribe",
    "consume",
    "ack",
    # Execution
    "execute_models",
    "build_dependency_graph",
    "get_connection",
    "sql",
    "run",
    "run_sync",
    # Logging
    "setup_logging",
    "setup_logging_from_config",
    "get_logger",
    # API utilities
    "API",
    "oauth2_token",
    "basic_auth_token",
    # Testing
    "test_model",
    "test_model_sync",
    "mock_dependency",
    "TestResult",
    # Config
    "config",
    # Exceptions
    "InterlaceError",
    "ConfigurationError",
    "InterlaceConnectionError",
    "ConnectionLockError",
    "ConnectionNotFoundError",
    "ExecutionError",
    "ModelExecutionError",
    "DependencyError",
    "DeadlockError",
    "InitializationError",
    "SchemaError",
    "MaterializationError",
    "QualityError",
    "RetryError",
    "CircuitBreakerOpenError",
    "StateStoreError",
]
