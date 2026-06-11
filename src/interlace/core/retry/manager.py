"""
Retry manager for executing functions with exponential backoff.

Wraps any callable (sync or async) with retry logic based on RetryPolicy.
"""

import asyncio
import time
from collections.abc import Callable
from typing import Any, TypeVar

from interlace.core.retry.policy import DEFAULT_RETRY_POLICY, RetryPolicy, RetryState
from interlace.utils.logging import get_logger

logger = get_logger("interlace.retry.manager")

T = TypeVar("T")


class RetryManager:
    """
    Manages retry logic for model execution with exponential backoff.

    Wraps any callable (sync or async) with retry logic based on RetryPolicy.
    Tracks retry history for observability and debugging.

    Examples:
        >>> manager = RetryManager()
        >>> async def fetch_data():
        ...     return await api.get('/data')
        >>> result = await manager.execute(fetch_data, policy=API_RETRY_POLICY)
    """

    def __init__(self) -> None:
        self._retry_stats: dict[str, Any] = {}

    async def execute(
        self,
        func: Callable[..., Any],
        *args: Any,
        policy: RetryPolicy | None = None,
        model_name: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Execute async function with retry logic.

        Args:
            func: Async function to execute
            *args: Positional arguments to pass to func
            policy: Retry policy (defaults to DEFAULT_RETRY_POLICY)
            model_name: Model name for logging
            **kwargs: Keyword arguments to pass to func

        Returns:
            Result of successful execution

        Raises:
            Exception: Final exception after all retries exhausted
        """
        policy = policy or DEFAULT_RETRY_POLICY
        state = RetryState(model_name=model_name or func.__name__)

        for attempt in range(policy.max_attempts + 1):
            state.attempt = attempt

            try:
                logger.debug(f"Executing {state.model_name} (attempt {attempt + 1}/{policy.max_attempts + 1})")

                result = await func(*args, **kwargs)

                state.record_attempt()
                state.mark_success(result)

                if attempt > 0:
                    logger.info(f"{state.model_name} succeeded after {attempt + 1} attempts")

                return result

            except Exception as e:
                state.record_attempt(exception=e)

                should_retry = attempt < policy.max_attempts and policy.should_retry(e, attempt)

                if not should_retry:
                    state.mark_failure(e)
                    logger.error(
                        f"{state.model_name} failed after {attempt + 1} attempts: {e}",
                        exc_info=True,
                    )
                    raise

                delay = policy.get_delay(attempt)
                state.record_delay(delay)

                logger.warning(f"{state.model_name} attempt {attempt + 1} failed: {e}. " f"Retrying in {delay:.2f}s...")

                await asyncio.sleep(delay)

        raise RuntimeError(f"Retry logic error for {state.model_name}")

    def execute_sync(
        self,
        func: Callable[..., T],
        *args: Any,
        policy: RetryPolicy | None = None,
        model_name: str | None = None,
        **kwargs: Any,
    ) -> T:
        """
        Execute sync function with retry logic.

        Args:
            func: Sync function to execute
            *args: Positional arguments to pass to func
            policy: Retry policy (defaults to DEFAULT_RETRY_POLICY)
            model_name: Model name for logging
            **kwargs: Keyword arguments to pass to func

        Returns:
            Result of successful execution

        Raises:
            Exception: Final exception after all retries exhausted
        """
        policy = policy or DEFAULT_RETRY_POLICY
        state = RetryState(model_name=model_name or func.__name__)

        for attempt in range(policy.max_attempts + 1):
            state.attempt = attempt

            try:
                logger.debug(f"Executing {state.model_name} (attempt {attempt + 1}/{policy.max_attempts + 1})")

                result = func(*args, **kwargs)

                state.record_attempt()
                state.mark_success(result)

                if attempt > 0:
                    logger.info(f"{state.model_name} succeeded after {attempt + 1} attempts")

                return result

            except Exception as e:
                state.record_attempt(exception=e)

                should_retry = attempt < policy.max_attempts and policy.should_retry(e, attempt)

                if not should_retry:
                    state.mark_failure(e)
                    logger.error(
                        f"{state.model_name} failed after {attempt + 1} attempts: {e}",
                        exc_info=True,
                    )
                    raise

                delay = policy.get_delay(attempt)
                state.record_delay(delay)

                logger.warning(f"{state.model_name} attempt {attempt + 1} failed: {e}. " f"Retrying in {delay:.2f}s...")

                time.sleep(delay)

        raise RuntimeError(f"Retry logic error for {state.model_name}")
