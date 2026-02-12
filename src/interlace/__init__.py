"""
Interlace - A modern, Python/SQL-first data pipeline framework.

Phase 0 (MVP): Core functionality for basic data pipeline execution.
"""

__version__ = "0.1.0"

# Core exports
from interlace.core.model import model
from interlace.core.stream import stream, publish, publish_sync, subscribe, consume, ack
from interlace.core.executor import execute_models
from interlace.core.dependencies import build_dependency_graph
from interlace.core.context import get_connection, sql

# Programmatic API
from interlace.core.api import run, run_sync

# Logging utilities
from interlace.utils.logging import setup_logging, setup_logging_from_config, get_logger

# API utilities
from interlace.utils.api import API, oauth2_token, basic_auth_token

# Testing utilities
from interlace.testing import test_model, test_model_sync, mock_dependency, TestResult

# Global config
from interlace.config.singleton import config

# Exceptions
from interlace.exceptions import (
    InterlaceError,
    ConfigurationError,
    InterlaceConnectionError,
    ConnectionLockError,
    ConnectionNotFoundError,
    ExecutionError,
    ModelExecutionError,
    DependencyError,
    DeadlockError,
    InitializationError,
    SchemaError,
    MaterializationError,
    QualityError,
    RetryError,
    CircuitBreakerOpenError,
    StateStoreError,
)

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
