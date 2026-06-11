"""
Tests for the retry framework.

Phase 2: Tests for retry policy and retry manager.
"""

import pytest

from interlace.core.retry import (
    API_RETRY_POLICY,
    DATABASE_RETRY_POLICY,
    DEFAULT_RETRY_POLICY,
    NO_RETRY_POLICY,
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


class TestPreConfiguredPolicies:
    """Tests for pre-configured retry policies."""

    def test_default_policy(self):
        """Test DEFAULT_RETRY_POLICY configuration."""
        assert DEFAULT_RETRY_POLICY.max_attempts == 3
        assert DEFAULT_RETRY_POLICY.initial_delay == 1.0

    def test_api_policy(self):
        """Test API_RETRY_POLICY configuration."""
        assert API_RETRY_POLICY.max_attempts == 5
        assert API_RETRY_POLICY.initial_delay == 2.0
        assert ConnectionError in API_RETRY_POLICY.retryable_exceptions

    def test_database_policy(self):
        """Test DATABASE_RETRY_POLICY configuration."""
        assert DATABASE_RETRY_POLICY.max_attempts == 3

    def test_no_retry_policy(self):
        """Test NO_RETRY_POLICY configuration."""
        assert NO_RETRY_POLICY.max_attempts == 0
