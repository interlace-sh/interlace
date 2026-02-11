"""
Retry framework for handling transient failures in model execution.

Phase 2: Production-grade retry with exponential backoff, circuit breaker, and DLQ.
"""

from interlace.core.retry.policy import (
    RetryPolicy,
    RetryState,
    DEFAULT_RETRY_POLICY,
    API_RETRY_POLICY,
    DATABASE_RETRY_POLICY,
    FAST_RETRY_POLICY,
    NO_RETRY_POLICY,
)
from interlace.core.retry.manager import RetryManager, CircuitBreakerOpenError
from interlace.core.retry.circuit_breaker import CircuitBreaker, CircuitState
from interlace.core.retry.dlq import DeadLetterQueue, DLQEntry

__all__ = [
    # Policy
    "RetryPolicy",
    "RetryState",
    "DEFAULT_RETRY_POLICY",
    "API_RETRY_POLICY",
    "DATABASE_RETRY_POLICY",
    "FAST_RETRY_POLICY",
    "NO_RETRY_POLICY",
    # Manager
    "RetryManager",
    "CircuitBreakerOpenError",
    # Circuit Breaker
    "CircuitBreaker",
    "CircuitState",
    # Dead Letter Queue
    "DeadLetterQueue",
    "DLQEntry",
]
