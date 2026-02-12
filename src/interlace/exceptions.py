"""
Interlace exception hierarchy.

All domain-specific exceptions inherit from InterlaceError, making it easy
to catch any framework error with a single base class while still allowing
fine-grained handling when needed.

Hierarchy::

    InterlaceError
    ├── ConfigurationError        - config loading, parsing, validation
    ├── ConnectionError_          - connection init, pooling, lock issues
    │   ├── ConnectionLockError   - database file locked by another process
    │   └── ConnectionNotFoundError - named connection not registered
    ├── ExecutionError            - model execution failures
    │   ├── ModelExecutionError   - single-model failure
    │   ├── DependencyError       - dependency resolution / loading
    │   └── DeadlockError         - DAG deadlock detected
    ├── InitializationError       - startup orchestration failures
    ├── SchemaError               - schema validation / evolution
    ├── MaterializationError      - table/view/ephemeral materialisation
    ├── QualityError              - data quality check failures
    ├── RetryError                - retry exhaustion, circuit breaker
    │   └── CircuitBreakerOpenError
    └── StateStoreError           - state database read/write
"""

from __future__ import annotations


class InterlaceError(Exception):
    """Base exception for all Interlace errors."""

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


# --- Configuration -----------------------------------------------------------


class ConfigurationError(InterlaceError):
    """Raised when configuration loading, parsing, or validation fails."""


# --- Connections -------------------------------------------------------------


class ConnectionError_(InterlaceError):
    """Raised when a database/storage connection cannot be established.

    Named with trailing underscore to avoid shadowing the builtin
    ``ConnectionError``; the public alias ``InterlaceConnectionError``
    is preferred for external use.
    """


# Public alias so callers don't need the underscore
InterlaceConnectionError = ConnectionError_


class ConnectionLockError(ConnectionError_):
    """Raised when a database file is locked by another process."""

    def __init__(self, message: str, *, pid: str | None = None, path: str | None = None) -> None:
        super().__init__(message, details={"pid": pid, "path": path})
        self.pid = pid
        self.path = path


class ConnectionNotFoundError(ConnectionError_):
    """Raised when a named connection is not registered."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Connection not found: {name}", details={"connection": name})
        self.connection_name = name


# --- Execution ---------------------------------------------------------------


class ExecutionError(InterlaceError):
    """Raised when model execution fails."""


class ModelExecutionError(ExecutionError):
    """Raised when a single model fails during execution."""

    def __init__(self, model_name: str, message: str, *, cause: Exception | None = None) -> None:
        full = f"Model '{model_name}' failed: {message}"
        super().__init__(full, details={"model": model_name})
        self.model_name = model_name
        if cause is not None:
            self.__cause__ = cause


class DependencyError(ExecutionError):
    """Raised when a dependency cannot be resolved or loaded."""

    def __init__(self, model_name: str, dependency: str, message: str) -> None:
        full = f"Dependency '{dependency}' for model '{model_name}': {message}"
        super().__init__(full, details={"model": model_name, "dependency": dependency})
        self.model_name = model_name
        self.dependency = dependency


class DeadlockError(ExecutionError):
    """Raised when the execution DAG is deadlocked."""


# --- Initialization ----------------------------------------------------------


class InitializationError(InterlaceError):
    """Raised during startup when a required component fails to initialize.

    Error messages should be informative and actionable. Exception chaining
    is intentionally suppressed (``from None``) to keep CLI output clean.
    """


# --- Schema ------------------------------------------------------------------


class SchemaError(InterlaceError):
    """Raised when schema validation or evolution fails."""


# --- Materialization ---------------------------------------------------------


class MaterializationError(InterlaceError):
    """Raised when table/view/ephemeral materialization fails."""


# --- Quality -----------------------------------------------------------------


class QualityError(InterlaceError):
    """Raised when data quality checks fail with error severity."""


# --- Retry -------------------------------------------------------------------


class RetryError(InterlaceError):
    """Raised when all retry attempts are exhausted."""


class CircuitBreakerOpenError(RetryError):
    """Raised when the circuit breaker is open and rejecting calls."""


# --- State store -------------------------------------------------------------


class StateStoreError(InterlaceError):
    """Raised when the state database cannot be read or written."""
