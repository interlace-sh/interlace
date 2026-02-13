"""
Tests for the retry framework.

Phase 2: Tests for retry policy, retry manager, circuit breaker, and DLQ.
"""

import time
from unittest.mock import MagicMock

import pytest

from interlace.core.retry import (
    API_RETRY_POLICY,
    DATABASE_RETRY_POLICY,
    DEFAULT_RETRY_POLICY,
    NO_RETRY_POLICY,
    CircuitBreaker,
    CircuitState,
    DeadLetterQueue,
    DLQEntry,
    RetryManager,
    RetryPolicy,
    RetryState,
)


class TestRetryPolicy:
    """Tests for RetryPolicy configuration."""

    def test_default_values(self):
        """Test default policy values."""
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.initial_delay == 1.0
        assert policy.max_delay == 30.0
        assert policy.exponential_base == 2.0
        assert policy.jitter is True
        assert policy.use_dlq is True

    def test_custom_values(self):
        """Test custom policy configuration."""
        policy = RetryPolicy(
            max_attempts=5,
            initial_delay=2.0,
            max_delay=60.0,
            exponential_base=3.0,
            jitter=False,
        )
        assert policy.max_attempts == 5
        assert policy.initial_delay == 2.0
        assert policy.max_delay == 60.0
        assert policy.exponential_base == 3.0
        assert policy.jitter is False

    def test_validation_max_attempts(self):
        """Test validation rejects negative max_attempts."""
        with pytest.raises(ValueError, match="max_attempts must be >= 0"):
            RetryPolicy(max_attempts=-1)

    def test_validation_initial_delay(self):
        """Test validation rejects non-positive initial_delay."""
        with pytest.raises(ValueError, match="initial_delay must be > 0"):
            RetryPolicy(initial_delay=0)

    def test_validation_max_delay(self):
        """Test validation rejects max_delay < initial_delay."""
        with pytest.raises(ValueError, match="max_delay must be >= initial_delay"):
            RetryPolicy(initial_delay=10.0, max_delay=5.0)

    def test_should_retry_within_attempts(self):
        """Test should_retry returns True within max_attempts."""
        policy = RetryPolicy(max_attempts=3)
        assert policy.should_retry(Exception("test"), attempt=0) is True
        assert policy.should_retry(Exception("test"), attempt=1) is True
        assert policy.should_retry(Exception("test"), attempt=2) is True
        assert policy.should_retry(Exception("test"), attempt=3) is False

    def test_should_retry_exception_filter(self):
        """Test should_retry with exception type filter."""
        policy = RetryPolicy(
            max_attempts=3,
            retryable_exceptions=(ConnectionError, TimeoutError),
        )
        # Retryable exceptions
        assert policy.should_retry(ConnectionError("test"), attempt=0) is True
        assert policy.should_retry(TimeoutError("test"), attempt=0) is True
        # Non-retryable exception
        assert policy.should_retry(ValueError("test"), attempt=0) is False

    def test_get_delay_exponential(self):
        """Test exponential backoff delay calculation."""
        policy = RetryPolicy(
            initial_delay=1.0,
            exponential_base=2.0,
            max_delay=30.0,
            jitter=False,
        )
        assert policy.get_delay(attempt=0) == 1.0  # 1.0 * 2^0
        assert policy.get_delay(attempt=1) == 2.0  # 1.0 * 2^1
        assert policy.get_delay(attempt=2) == 4.0  # 1.0 * 2^2
        assert policy.get_delay(attempt=3) == 8.0  # 1.0 * 2^3

    def test_get_delay_max_cap(self):
        """Test delay is capped at max_delay."""
        policy = RetryPolicy(
            initial_delay=1.0,
            exponential_base=2.0,
            max_delay=5.0,
            jitter=False,
        )
        assert policy.get_delay(attempt=10) == 5.0  # Capped at max_delay

    def test_get_delay_jitter(self):
        """Test jitter adds randomness to delay."""
        policy = RetryPolicy(
            initial_delay=10.0,
            jitter=True,
        )
        # With jitter, delay should be within ±25% of base
        delays = [policy.get_delay(attempt=0) for _ in range(100)]
        assert min(delays) >= 7.5  # 10 * 0.75
        assert max(delays) <= 12.5  # 10 * 1.25
        # Ensure there's variance (not all the same)
        assert len(set(delays)) > 1


class TestRetryState:
    """Tests for RetryState tracking."""

    def test_record_attempt(self):
        """Test recording attempts."""
        state = RetryState(model_name="test_model")
        state.record_attempt()
        assert state.total_attempts == 1
        assert len(state.attempt_timestamps) == 1

    def test_record_attempt_with_exception(self):
        """Test recording attempts with exceptions."""
        state = RetryState(model_name="test_model")
        state.record_attempt(exception=ValueError("test error"))
        assert len(state.exceptions) == 1
        assert state.exceptions[0]["exception_type"] == "ValueError"
        assert state.exceptions[0]["exception_message"] == "test error"

    def test_mark_success(self):
        """Test marking execution as successful."""
        state = RetryState(model_name="test_model")
        state.mark_success(result={"data": "test"})
        assert state.succeeded is True
        assert state.result == {"data": "test"}

    def test_mark_failure(self):
        """Test marking execution as failed."""
        state = RetryState(model_name="test_model")
        exc = ValueError("final error")
        state.mark_failure(exc)
        assert state.succeeded is False
        assert state.final_exception is exc


class TestRetryManager:
    """Tests for RetryManager execution."""

    @pytest.mark.asyncio
    async def test_execute_success_first_attempt(self):
        """Test successful execution on first attempt."""
        manager = RetryManager()
        call_count = 0

        async def success_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = await manager.execute(success_func, policy=DEFAULT_RETRY_POLICY)
        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_execute_success_after_retry(self):
        """Test successful execution after retry."""
        manager = RetryManager()
        call_count = 0

        async def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("temporary failure")
            return "success"

        policy = RetryPolicy(max_attempts=5, initial_delay=0.01, jitter=False)
        result = await manager.execute(fail_then_succeed, policy=policy)
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_execute_all_retries_exhausted(self):
        """Test exception raised when all retries exhausted."""
        manager = RetryManager()

        async def always_fail():
            raise ValueError("permanent failure")

        policy = RetryPolicy(max_attempts=2, initial_delay=0.01, jitter=False)
        with pytest.raises(ValueError, match="permanent failure"):
            await manager.execute(always_fail, policy=policy)

    @pytest.mark.asyncio
    async def test_execute_no_retry_policy(self):
        """Test execution with no retry (max_attempts=0)."""
        manager = RetryManager()
        call_count = 0

        async def fail_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("failure")

        with pytest.raises(ValueError):
            await manager.execute(fail_func, policy=NO_RETRY_POLICY)
        assert call_count == 1

    def test_execute_sync_success(self):
        """Test sync execution success."""
        manager = RetryManager()
        call_count = 0

        def success_func():
            nonlocal call_count
            call_count += 1
            return "sync success"

        result = manager.execute_sync(success_func, policy=DEFAULT_RETRY_POLICY)
        assert result == "sync success"
        assert call_count == 1

    def test_execute_sync_with_retry(self):
        """Test sync execution with retry."""
        manager = RetryManager()
        call_count = 0

        def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("temp failure")
            return "success"

        policy = RetryPolicy(max_attempts=3, initial_delay=0.01, jitter=False)
        result = manager.execute_sync(fail_then_succeed, policy=policy)
        assert result == "success"
        assert call_count == 2


class TestCircuitBreaker:
    """Tests for CircuitBreaker pattern."""

    def test_initial_state_closed(self):
        """Test circuit breaker starts in CLOSED state."""
        breaker = CircuitBreaker()
        assert breaker.state == CircuitState.CLOSED

    def test_allow_request_when_closed(self):
        """Test requests allowed when circuit is closed."""
        breaker = CircuitBreaker()
        assert breaker.allow_request() is True

    def test_opens_after_failure_threshold(self):
        """Test circuit opens after failure threshold reached."""
        breaker = CircuitBreaker(failure_threshold=3)

        # Record failures
        breaker.record_failure()
        assert breaker.state == CircuitState.CLOSED
        breaker.record_failure()
        assert breaker.state == CircuitState.CLOSED
        breaker.record_failure()
        assert breaker.state == CircuitState.OPEN

    def test_blocks_requests_when_open(self):
        """Test requests blocked when circuit is open."""
        breaker = CircuitBreaker(failure_threshold=1)
        breaker.record_failure()
        assert breaker.state == CircuitState.OPEN
        assert breaker.allow_request() is False

    def test_half_open_after_timeout(self):
        """Test circuit enters half-open after timeout."""
        breaker = CircuitBreaker(failure_threshold=1, timeout=0.1)
        breaker.record_failure()
        assert breaker.state == CircuitState.OPEN

        # Wait for timeout
        time.sleep(0.15)
        assert breaker.state == CircuitState.HALF_OPEN

    def test_closes_after_success_in_half_open(self):
        """Test circuit closes after success in half-open state."""
        breaker = CircuitBreaker(
            failure_threshold=1,
            timeout=0.01,
            success_threshold=2,
        )
        breaker.record_failure()
        time.sleep(0.02)

        # Half-open state
        assert breaker.state == CircuitState.HALF_OPEN

        # Record successes
        breaker.record_success()
        assert breaker.state == CircuitState.HALF_OPEN
        breaker.record_success()
        assert breaker.state == CircuitState.CLOSED

    def test_reopens_on_failure_in_half_open(self):
        """Test circuit reopens on failure in half-open state."""
        breaker = CircuitBreaker(failure_threshold=1, timeout=0.01)
        breaker.record_failure()
        time.sleep(0.02)

        assert breaker.state == CircuitState.HALF_OPEN
        breaker.record_failure()
        assert breaker.state == CircuitState.OPEN

    def test_reset(self):
        """Test manual reset closes circuit."""
        breaker = CircuitBreaker(failure_threshold=1)
        breaker.record_failure()
        assert breaker.state == CircuitState.OPEN

        breaker.reset()
        assert breaker.state == CircuitState.CLOSED

    def test_get_stats(self):
        """Test stats reporting."""
        breaker = CircuitBreaker(failure_threshold=3)
        breaker.record_failure()
        breaker.record_failure()

        stats = breaker.get_stats()
        assert stats["state"] == "closed"
        assert stats["failure_count"] == 2


class TestDeadLetterQueue:
    """Tests for Dead Letter Queue."""

    def test_add_entry(self):
        """Test adding entry to DLQ."""
        dlq = DeadLetterQueue()

        entry = DLQEntry(
            model_name="test_model",
            exception_type="ValueError",
            exception_message="test error",
            total_attempts=3,
        )

        dlq_id = dlq.add(entry)
        assert dlq_id is not None
        assert entry.dlq_id == dlq_id

    def test_get_by_id(self):
        """Test retrieving entry by ID."""
        dlq = DeadLetterQueue()

        entry = DLQEntry(
            model_name="test_model",
            exception_type="ValueError",
            exception_message="test error",
        )
        dlq_id = dlq.add(entry)

        retrieved = dlq.get_by_id(dlq_id)
        assert retrieved is not None
        assert retrieved.model_name == "test_model"

    def test_get_by_model(self):
        """Test retrieving entries by model name."""
        dlq = DeadLetterQueue()

        # Add entries for different models
        dlq.add(
            DLQEntry(
                model_name="model_a",
                exception_type="ValueError",
                exception_message="error 1",
            )
        )
        dlq.add(
            DLQEntry(
                model_name="model_a",
                exception_type="ValueError",
                exception_message="error 2",
            )
        )
        dlq.add(
            DLQEntry(
                model_name="model_b",
                exception_type="ValueError",
                exception_message="error 3",
            )
        )

        entries = dlq.get_by_model("model_a")
        assert len(entries) == 2
        assert all(e.model_name == "model_a" for e in entries)

    def test_get_recent(self):
        """Test retrieving recent entries."""
        dlq = DeadLetterQueue()

        for i in range(5):
            dlq.add(
                DLQEntry(
                    model_name=f"model_{i}",
                    exception_type="ValueError",
                    exception_message=f"error {i}",
                )
            )

        entries = dlq.get_recent(limit=3)
        assert len(entries) == 3

    def test_mark_resolved(self):
        """Test marking entry as resolved."""
        dlq = DeadLetterQueue()

        entry = DLQEntry(
            model_name="test_model",
            exception_type="ValueError",
            exception_message="test error",
        )
        dlq_id = dlq.add(entry)

        dlq.mark_resolved(dlq_id)

        # Entry should be removed
        assert dlq.get_by_id(dlq_id) is None

    def test_clear_model(self):
        """Test clearing all entries for a model."""
        dlq = DeadLetterQueue()

        dlq.add(DLQEntry(model_name="model_a", exception_type="E", exception_message="1"))
        dlq.add(DLQEntry(model_name="model_a", exception_type="E", exception_message="2"))
        dlq.add(DLQEntry(model_name="model_b", exception_type="E", exception_message="3"))

        dlq.clear_model("model_a")

        assert len(dlq.get_by_model("model_a")) == 0
        assert len(dlq.get_by_model("model_b")) == 1

    def test_get_stats(self):
        """Test DLQ statistics."""
        dlq = DeadLetterQueue()

        dlq.add(DLQEntry(model_name="m1", exception_type="ValueError", exception_message="e1"))
        dlq.add(DLQEntry(model_name="m1", exception_type="TypeError", exception_message="e2"))
        dlq.add(DLQEntry(model_name="m2", exception_type="ValueError", exception_message="e3"))

        stats = dlq.get_stats()
        assert stats["total_entries"] == 3
        assert stats["entries_by_model"]["m1"] == 2
        assert stats["entries_by_model"]["m2"] == 1
        assert stats["entries_by_exception"]["ValueError"] == 2
        assert stats["entries_by_exception"]["TypeError"] == 1


class TestDLQEntry:
    """Tests for DLQ entry serialization."""

    def test_to_dict(self):
        """Test conversion to dictionary."""
        entry = DLQEntry(
            model_name="test_model",
            exception_type="ValueError",
            exception_message="test error",
            total_attempts=3,
        )

        d = entry.to_dict()
        assert d["model_name"] == "test_model"
        assert d["exception_type"] == "ValueError"
        assert d["total_attempts"] == 3

    def test_to_json(self):
        """Test conversion to JSON."""
        entry = DLQEntry(
            model_name="test_model",
            exception_type="ValueError",
            exception_message="test error",
        )

        json_str = entry.to_json()
        assert "test_model" in json_str
        assert "ValueError" in json_str

    def test_from_dict(self):
        """Test creation from dictionary."""
        data = {
            "model_name": "test_model",
            "exception_type": "ValueError",
            "exception_message": "test error",
            "total_attempts": 5,
            "retry_history": [],
            "model_config": {},
            "model_dependencies": [],
            "dlq_timestamp": 1234567890.0,
            "first_attempt_time": 1234567890.0,
            "last_attempt_time": 1234567890.0,
        }

        entry = DLQEntry.from_dict(data)
        assert entry.model_name == "test_model"
        assert entry.total_attempts == 5


class TestPreConfiguredPolicies:
    """Tests for pre-configured retry policies."""

    def test_default_policy(self):
        """Test DEFAULT_RETRY_POLICY configuration."""
        assert DEFAULT_RETRY_POLICY.max_attempts == 3
        assert DEFAULT_RETRY_POLICY.initial_delay == 1.0
        assert DEFAULT_RETRY_POLICY.use_dlq is True

    def test_api_policy(self):
        """Test API_RETRY_POLICY configuration."""
        assert API_RETRY_POLICY.max_attempts == 5
        assert API_RETRY_POLICY.initial_delay == 2.0
        assert ConnectionError in API_RETRY_POLICY.retryable_exceptions

    def test_database_policy(self):
        """Test DATABASE_RETRY_POLICY configuration."""
        assert DATABASE_RETRY_POLICY.max_attempts == 3
        assert DATABASE_RETRY_POLICY.circuit_breaker_threshold == 10

    def test_no_retry_policy(self):
        """Test NO_RETRY_POLICY configuration."""
        assert NO_RETRY_POLICY.max_attempts == 0
        assert NO_RETRY_POLICY.use_dlq is False


class TestDLQDatabasePersistence:
    """Tests for DLQ database persistence."""

    def test_persist_with_mock_state_store(self):
        """Test persistence with mocked state store."""
        # Create mock state store with mock connection
        mock_conn = MagicMock()
        mock_state_store = MagicMock()
        mock_state_store._get_connection.return_value = mock_conn

        dlq = DeadLetterQueue(state_store=mock_state_store)

        entry = DLQEntry(
            model_name="test_model",
            exception_type="ValueError",
            exception_message="test error",
        )

        # Should not raise, even if SQL fails (fallback to in-memory)
        dlq_id = dlq.add(entry)
        assert dlq_id is not None

    def test_fallback_to_memory_on_db_error(self):
        """Test fallback to in-memory when database fails."""
        # Create mock state store that returns None connection
        mock_state_store = MagicMock()
        mock_state_store._get_connection.return_value = None

        dlq = DeadLetterQueue(state_store=mock_state_store)

        entry = DLQEntry(
            model_name="test_model",
            exception_type="ValueError",
            exception_message="test error",
        )

        dlq_id = dlq.add(entry)
        assert dlq_id is not None

        # Entry should be in in-memory queue
        assert len(dlq._in_memory_queue) == 1

    def test_row_to_entry_conversion(self):
        """Test database row to DLQEntry conversion."""
        dlq = DeadLetterQueue()

        # Mock row data (as would come from pandas DataFrame)
        class MockRow:
            def get(self, key, default=None):
                data = {
                    "dlq_id": "dlq_abc123",
                    "model_name": "test_model",
                    "exception_type": "ValueError",
                    "exception_message": "test error",
                    "exception_traceback": "traceback here",
                    "total_attempts": 3,
                    "retry_history": "[]",
                    "first_attempt_time": 1234567890.0,
                    "last_attempt_time": 1234567899.0,
                    "total_duration": 9.0,
                    "model_config": '{"key": "value"}',
                    "model_dependencies": '["dep1", "dep2"]',
                    "run_id": "run_123",
                    "environment": "test",
                    "dlq_timestamp": 1234567900.0,
                }
                return data.get(key, default)

        entry = dlq._row_to_entry(MockRow())
        assert entry.dlq_id == "dlq_abc123"
        assert entry.model_name == "test_model"
        assert entry.exception_type == "ValueError"
        assert entry.total_attempts == 3
        assert entry.model_config == {"key": "value"}
        assert entry.model_dependencies == ["dep1", "dep2"]
