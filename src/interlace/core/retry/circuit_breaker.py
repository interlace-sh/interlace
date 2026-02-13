"""
Circuit breaker pattern for fail-fast behavior.

Phase 2: Prevents cascading failures by stopping requests to failing services.

Based on Martin Fowler's Circuit Breaker pattern:
https://martinfowler.com/bliki/CircuitBreaker.html
"""

import time
from enum import StrEnum

from interlace.exceptions import CircuitBreakerOpenError  # noqa: F401 - re-export
from interlace.utils.logging import get_logger

logger = get_logger("interlace.retry.circuit_breaker")


class CircuitState(StrEnum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation, requests allowed
    OPEN = "open"  # Failure threshold exceeded, requests blocked
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """
    Circuit breaker for fail-fast behavior.

    Tracks failures and opens the circuit (rejects requests) when failure
    threshold is exceeded. After a timeout, enters half-open state to test
    if service has recovered.

    States:
    - CLOSED: Normal operation, all requests allowed
    - OPEN: Too many failures, all requests blocked
    - HALF_OPEN: Testing recovery, limited requests allowed

    Examples:
        >>> breaker = CircuitBreaker(
        ...     failure_threshold=5,
        ...     timeout=60.0,
        ...     half_open_max_requests=3
        ... )
        >>>
        >>> if breaker.allow_request():
        ...     try:
        ...         result = call_external_service()
        ...         breaker.record_success()
        ...     except Exception:
        ...         breaker.record_failure()
        ... else:
        ...     # Circuit is open, fail fast
        ...     raise CircuitBreakerOpenError()
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        timeout: float = 60.0,
        half_open_max_requests: int = 3,
        success_threshold: int = 2,
    ):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Number of consecutive failures before opening circuit
            timeout: Seconds before attempting to close circuit (OPEN → HALF_OPEN)
            half_open_max_requests: Max requests to allow in HALF_OPEN state
            success_threshold: Consecutive successes needed in HALF_OPEN to close circuit
        """
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.half_open_max_requests = half_open_max_requests
        self.success_threshold = success_threshold

        # State tracking
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._half_open_requests = 0

    @property
    def state(self) -> CircuitState:
        """Current circuit breaker state."""
        self._update_state()
        return self._state

    def allow_request(self) -> bool:
        """
        Check if request should be allowed.

        Returns:
            True if request should proceed, False if circuit is open
        """
        self._update_state()

        if self._state == CircuitState.CLOSED:
            return True

        if self._state == CircuitState.OPEN:
            return False

        # HALF_OPEN: Allow limited requests
        if self._half_open_requests < self.half_open_max_requests:
            self._half_open_requests += 1
            return True

        return False

    def record_success(self) -> None:
        """Record a successful request."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1

            if self._success_count >= self.success_threshold:
                # Service recovered, close circuit
                self._close_circuit()
                logger.info("Circuit breaker closed after successful recovery")

        elif self._state == CircuitState.CLOSED:
            # Reset failure count on success
            self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed request."""
        if self._state == CircuitState.OPEN:
            # Don't process failures when circuit is already open.
            # Updating _last_failure_time here would reset the timeout
            # for the HALF_OPEN transition, preventing recovery.
            return

        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._state == CircuitState.CLOSED:
            if self._failure_count >= self.failure_threshold:
                # Too many failures, open circuit
                self._open_circuit()
                logger.warning(f"Circuit breaker opened after {self._failure_count} consecutive failures")

        elif self._state == CircuitState.HALF_OPEN:
            # Failure during recovery, re-open circuit
            self._open_circuit()
            logger.warning("Circuit breaker re-opened after failure during recovery")

    def _update_state(self) -> None:
        """Update circuit state based on timeout."""
        if self._state == CircuitState.OPEN:
            if self._last_failure_time is not None:
                elapsed = time.time() - self._last_failure_time

                if elapsed >= self.timeout:
                    # Timeout elapsed, try half-open
                    self._half_open_circuit()
                    logger.info("Circuit breaker entering half-open state")

    def _open_circuit(self) -> None:
        """Transition to OPEN state."""
        self._state = CircuitState.OPEN
        self._success_count = 0
        self._half_open_requests = 0

    def _half_open_circuit(self) -> None:
        """Transition to HALF_OPEN state."""
        self._state = CircuitState.HALF_OPEN
        self._failure_count = 0
        self._success_count = 0
        self._half_open_requests = 0

    def _close_circuit(self) -> None:
        """Transition to CLOSED state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_requests = 0
        self._last_failure_time = None

    def reset(self) -> None:
        """Manually reset circuit breaker to CLOSED state."""
        self._close_circuit()
        logger.info("Circuit breaker manually reset")

    def get_stats(self) -> dict:
        """
        Get circuit breaker statistics.

        Returns:
            Dictionary with current state and metrics
        """
        return {
            "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time,
            "half_open_requests": self._half_open_requests,
            "time_until_half_open": (
                max(0, self.timeout - (time.time() - self._last_failure_time))
                if self._state == CircuitState.OPEN and self._last_failure_time
                else None
            ),
        }
