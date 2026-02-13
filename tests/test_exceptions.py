"""
Tests for the exception hierarchy.
"""

import pytest

from interlace.exceptions import (
    CircuitBreakerOpenError,
    ConfigurationError,
    ConnectionError_,
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


class TestHierarchy:
    """Verify all exceptions inherit from InterlaceError."""

    @pytest.mark.parametrize(
        "exc_class",
        [
            ConfigurationError,
            ConnectionError_,
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
        ],
    )
    def test_inherits_from_interlace_error(self, exc_class):
        assert issubclass(exc_class, InterlaceError)

    def test_connection_alias(self):
        assert InterlaceConnectionError is ConnectionError_

    def test_circuit_breaker_inherits_retry(self):
        assert issubclass(CircuitBreakerOpenError, RetryError)

    def test_model_execution_inherits_execution(self):
        assert issubclass(ModelExecutionError, ExecutionError)

    def test_dependency_inherits_execution(self):
        assert issubclass(DependencyError, ExecutionError)


class TestExceptionMessages:
    """Test exception constructors and details."""

    def test_interlace_error(self):
        e = InterlaceError("boom", details={"key": "val"})
        assert str(e) == "boom"
        assert e.message == "boom"
        assert e.details == {"key": "val"}

    def test_connection_lock_error(self):
        e = ConnectionLockError("locked", pid="1234", path="/tmp/db")
        assert "locked" in str(e)
        assert e.pid == "1234"
        assert e.path == "/tmp/db"
        assert e.details["pid"] == "1234"

    def test_connection_not_found_error(self):
        e = ConnectionNotFoundError("my_conn")
        assert "my_conn" in str(e)
        assert e.connection_name == "my_conn"

    def test_model_execution_error(self):
        cause = ValueError("bad data")
        e = ModelExecutionError("my_model", "crashed", cause=cause)
        assert "my_model" in str(e)
        assert "crashed" in str(e)
        assert e.model_name == "my_model"
        assert e.__cause__ is cause

    def test_dependency_error(self):
        e = DependencyError("orders", "users", "table not found")
        assert "orders" in str(e)
        assert "users" in str(e)
        assert e.model_name == "orders"
        assert e.dependency == "users"

    def test_catchable_with_base(self):
        """All exceptions can be caught with InterlaceError."""
        with pytest.raises(InterlaceError):
            raise ConfigurationError("bad config")

        with pytest.raises(InterlaceError):
            raise ConnectionLockError("locked", pid="1")
