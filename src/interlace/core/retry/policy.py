"""
Retry policy configuration for model execution.

Phase 2: Retry framework with exponential backoff for production resilience.
"""

from dataclasses import dataclass, field
from typing import Optional, Tuple, Type
import asyncio


@dataclass
class RetryPolicy:
    """
    Configuration for retry behavior when model execution fails.

    Implements exponential backoff with jitter for handling transient failures.
    Similar to Airflow/Dagster retry mechanisms but integrated into model execution.

    Examples:
        >>> # Basic retry with defaults
        >>> policy = RetryPolicy(max_attempts=3)

        >>> # Advanced configuration
        >>> policy = RetryPolicy(
        ...     max_attempts=5,
        ...     initial_delay=2.0,
        ...     max_delay=60.0,
        ...     exponential_base=2.0,
        ...     jitter=True,
        ...     retryable_exceptions=(ConnectionError, TimeoutError)
        ... )

        >>> # Retry specific errors only
        >>> import requests
        >>> policy = RetryPolicy(
        ...     max_attempts=3,
        ...     retryable_exceptions=(requests.RequestException,)
        ... )
    """

    # Maximum number of retry attempts (total executions = max_attempts + 1)
    max_attempts: int = 3

    # Initial delay before first retry (seconds)
    initial_delay: float = 1.0

    # Maximum delay between retries (seconds)
    max_delay: float = 30.0

    # Exponential backoff base (delay = initial_delay * base^attempt)
    exponential_base: float = 2.0

    # Add random jitter to prevent thundering herd (±25% of delay)
    jitter: bool = True

    # Only retry these exception types (None = retry all exceptions)
    retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None

    # Circuit breaker: fail fast after N consecutive failures across all models
    circuit_breaker_threshold: Optional[int] = None

    # Circuit breaker: reset after this many seconds
    circuit_breaker_timeout: float = 300.0

    # Dead letter queue: store failed runs for manual inspection
    use_dlq: bool = True

    # Retry on specific exit codes (for subprocess/shell command failures)
    retry_exit_codes: Optional[Tuple[int, ...]] = None

    # Custom retry condition function (optional, advanced use)
    # Signature: (exception: Exception, attempt: int) -> bool
    retry_condition: Optional[callable] = None

    def __post_init__(self):
        """Validate configuration."""
        if self.max_attempts < 0:
            raise ValueError("max_attempts must be >= 0")
        if self.initial_delay <= 0:
            raise ValueError("initial_delay must be > 0")
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be >= initial_delay")
        if self.exponential_base < 1.0:
            raise ValueError("exponential_base must be >= 1.0")
        if self.circuit_breaker_threshold is not None and self.circuit_breaker_threshold < 1:
            raise ValueError("circuit_breaker_threshold must be >= 1")

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """
        Determine if we should retry after this exception.

        Args:
            exception: The exception that occurred
            attempt: Current attempt number (0-indexed)

        Returns:
            True if we should retry, False otherwise
        """
        # Check if we've exceeded max attempts
        if attempt >= self.max_attempts:
            return False

        # Custom retry condition takes precedence
        if self.retry_condition is not None:
            return self.retry_condition(exception, attempt)

        # Check if exception type is retryable
        if self.retryable_exceptions is not None:
            return isinstance(exception, self.retryable_exceptions)

        # By default, retry all exceptions
        return True

    def get_delay(self, attempt: int) -> float:
        """
        Calculate delay before next retry using exponential backoff.

        Implements: delay = min(initial_delay * base^attempt, max_delay)
        With optional jitter: delay * random(0.75, 1.25)

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds before next retry
        """
        import random

        # Calculate exponential delay
        delay = self.initial_delay * (self.exponential_base ** attempt)

        # Add jitter to prevent thundering herd
        if self.jitter:
            # Random jitter: ±25% of delay
            jitter_factor = random.uniform(0.75, 1.25)
            delay *= jitter_factor

        # Cap at max_delay (applied after jitter so max_delay is a hard upper bound)
        delay = min(delay, self.max_delay)

        return delay


@dataclass
class RetryState:
    """
    State tracking for retry execution.

    Stores retry history and metadata for observability and debugging.
    """

    # Model name being retried
    model_name: str

    # Current attempt number (0-indexed)
    attempt: int = 0

    # Total attempts so far
    total_attempts: int = 0

    # Exceptions encountered (for debugging)
    exceptions: list = field(default_factory=list)

    # Timestamps of each attempt
    attempt_timestamps: list = field(default_factory=list)

    # Delays between attempts
    delays: list = field(default_factory=list)

    # Final result (if succeeded)
    result: Optional[any] = None

    # Final exception (if all retries failed)
    final_exception: Optional[Exception] = None

    # Whether execution succeeded
    succeeded: bool = False

    def record_attempt(self, exception: Optional[Exception] = None):
        """Record an attempt and its result."""
        import time

        self.total_attempts += 1
        self.attempt_timestamps.append(time.time())

        if exception is not None:
            self.exceptions.append({
                'attempt': self.attempt,
                'exception_type': type(exception).__name__,
                'exception_message': str(exception),
                'timestamp': time.time()
            })

    def record_delay(self, delay: float):
        """Record the delay before next retry."""
        self.delays.append(delay)

    def mark_success(self, result: any):
        """Mark execution as successful."""
        self.succeeded = True
        self.result = result

    def mark_failure(self, exception: Exception):
        """Mark execution as failed after all retries."""
        self.succeeded = False
        self.final_exception = exception


# Pre-configured policies for common scenarios

DEFAULT_RETRY_POLICY = RetryPolicy(
    max_attempts=3,
    initial_delay=1.0,
    max_delay=30.0,
    exponential_base=2.0,
    jitter=True,
    use_dlq=True
)

API_RETRY_POLICY = RetryPolicy(
    max_attempts=5,
    initial_delay=2.0,
    max_delay=60.0,
    exponential_base=2.0,
    jitter=True,
    retryable_exceptions=(
        ConnectionError,
        TimeoutError,
        OSError,  # Network errors
    ),
    use_dlq=True
)

DATABASE_RETRY_POLICY = RetryPolicy(
    max_attempts=3,
    initial_delay=0.5,
    max_delay=10.0,
    exponential_base=2.0,
    jitter=True,
    retryable_exceptions=(
        ConnectionError,
        TimeoutError,
    ),
    circuit_breaker_threshold=10,  # Fail fast if DB is down
    use_dlq=True
)

FAST_RETRY_POLICY = RetryPolicy(
    max_attempts=2,
    initial_delay=0.1,
    max_delay=1.0,
    exponential_base=2.0,
    jitter=False,
    use_dlq=False
)

NO_RETRY_POLICY = RetryPolicy(
    max_attempts=0,
    use_dlq=False
)
