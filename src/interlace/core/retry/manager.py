"""
Retry manager for executing functions with exponential backoff.

Phase 2: Retry framework implementation with async support.
"""

import asyncio
import time
from collections.abc import Callable
from typing import Any, Optional, TypeVar

from interlace.core.retry.circuit_breaker import CircuitBreaker
from interlace.core.retry.policy import DEFAULT_RETRY_POLICY, RetryPolicy, RetryState
from interlace.exceptions import CircuitBreakerOpenError
from interlace.utils.logging import get_logger

logger = get_logger("interlace.retry.manager")

T = TypeVar("T")


class RetryManager:
    """
    Manages retry logic for model execution with exponential backoff.

    Wraps any callable (sync or async) with retry logic based on RetryPolicy.
    Tracks retry history and integrates with circuit breaker and DLQ.

    Examples:
        >>> # Basic usage with async function
        >>> manager = RetryManager()
        >>> async def fetch_data():
        ...     # May fail with transient errors
        ...     return await api.get('/data')
        >>> result = await manager.execute(fetch_data, policy=API_RETRY_POLICY)

        >>> # Usage with sync function
        >>> def process_file():
        ...     return pd.read_csv('data.csv')
        >>> result = manager.execute_sync(process_file, policy=DEFAULT_RETRY_POLICY)

        >>> # Custom policy
        >>> policy = RetryPolicy(max_attempts=5, initial_delay=2.0)
        >>> result = await manager.execute(my_function, policy=policy)
    """

    def __init__(self, circuit_breaker: Optional["CircuitBreaker"] = None):
        """
        Initialize RetryManager.

        Args:
            circuit_breaker: Optional circuit breaker for fail-fast behavior
        """
        self.circuit_breaker = circuit_breaker
        self._retry_stats = {}  # Track retry statistics per model

    async def execute(
        self,
        func: Callable[..., Any],
        *args,
        policy: RetryPolicy | None = None,
        model_name: str | None = None,
        **kwargs,
    ) -> Any:
        """
        Execute async function with retry logic.

        Args:
            func: Async function to execute
            *args: Positional arguments to pass to func
            policy: Retry policy (defaults to DEFAULT_RETRY_POLICY)
            model_name: Model name for logging and DLQ
            **kwargs: Keyword arguments to pass to func

        Returns:
            Result of successful execution

        Raises:
            Exception: Final exception after all retries exhausted
        """
        policy = policy or DEFAULT_RETRY_POLICY
        state = RetryState(model_name=model_name or func.__name__)

        # Check circuit breaker before attempting
        if self.circuit_breaker and not self.circuit_breaker.allow_request():
            logger.warning(f"Circuit breaker open for {state.model_name}, failing fast")
            raise CircuitBreakerOpenError(f"Circuit breaker open, not attempting {state.model_name}")

        for attempt in range(policy.max_attempts + 1):
            state.attempt = attempt

            try:
                # Execute the function
                logger.debug(f"Executing {state.model_name} (attempt {attempt + 1}/{policy.max_attempts + 1})")

                result = await func(*args, **kwargs)

                # Success!
                state.record_attempt()
                state.mark_success(result)

                # Record success with circuit breaker
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()

                # Log retry statistics if retries occurred
                if attempt > 0:
                    logger.info(f"{state.model_name} succeeded after {attempt + 1} attempts")

                return result

            except Exception as e:
                # Record the failed attempt
                state.record_attempt(exception=e)

                # Check if we should retry
                should_retry = attempt < policy.max_attempts and policy.should_retry(e, attempt)

                if not should_retry:
                    # No more retries, fail
                    state.mark_failure(e)

                    # Record failure with circuit breaker
                    if self.circuit_breaker:
                        self.circuit_breaker.record_failure()

                    logger.error(
                        f"{state.model_name} failed after {attempt + 1} attempts: {e}",
                        exc_info=True,
                    )

                    raise

                # Calculate delay and retry
                delay = policy.get_delay(attempt)
                state.record_delay(delay)

                logger.warning(f"{state.model_name} attempt {attempt + 1} failed: {e}. " f"Retrying in {delay:.2f}s...")

                await asyncio.sleep(delay)

        # Should not reach here, but just in case
        raise RuntimeError(f"Retry logic error for {state.model_name}")

    def execute_sync(
        self,
        func: Callable[..., T],
        *args,
        policy: RetryPolicy | None = None,
        model_name: str | None = None,
        **kwargs,
    ) -> T:
        """
        Execute sync function with retry logic.

        Args:
            func: Sync function to execute
            *args: Positional arguments to pass to func
            policy: Retry policy (defaults to DEFAULT_RETRY_POLICY)
            model_name: Model name for logging and DLQ
            **kwargs: Keyword arguments to pass to func

        Returns:
            Result of successful execution

        Raises:
            Exception: Final exception after all retries exhausted
        """
        policy = policy or DEFAULT_RETRY_POLICY
        state = RetryState(model_name=model_name or func.__name__)

        # Check circuit breaker before attempting
        if self.circuit_breaker and not self.circuit_breaker.allow_request():
            logger.warning(f"Circuit breaker open for {state.model_name}, failing fast")
            raise CircuitBreakerOpenError(f"Circuit breaker open, not attempting {state.model_name}")

        for attempt in range(policy.max_attempts + 1):
            state.attempt = attempt

            try:
                # Execute the function
                logger.debug(f"Executing {state.model_name} (attempt {attempt + 1}/{policy.max_attempts + 1})")

                result = func(*args, **kwargs)

                # Success!
                state.record_attempt()
                state.mark_success(result)

                # Record success with circuit breaker
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()

                # Log retry statistics if retries occurred
                if attempt > 0:
                    logger.info(f"{state.model_name} succeeded after {attempt + 1} attempts")

                return result

            except Exception as e:
                # Record the failed attempt
                state.record_attempt(exception=e)

                # Check if we should retry
                should_retry = attempt < policy.max_attempts and policy.should_retry(e, attempt)

                if not should_retry:
                    # No more retries, fail
                    state.mark_failure(e)

                    # Record failure with circuit breaker
                    if self.circuit_breaker:
                        self.circuit_breaker.record_failure()

                    logger.error(
                        f"{state.model_name} failed after {attempt + 1} attempts: {e}",
                        exc_info=True,
                    )

                    raise

                # Calculate delay and retry
                delay = policy.get_delay(attempt)
                state.record_delay(delay)

                logger.warning(f"{state.model_name} attempt {attempt + 1} failed: {e}. " f"Retrying in {delay:.2f}s...")

                time.sleep(delay)

        # Should not reach here, but just in case
        raise RuntimeError(f"Retry logic error for {state.model_name}")
