"""
Async utilities for Interlace.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

T = TypeVar("T")


def dual[T](func: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    """
    Decorator: makes an async function callable both synchronously and asynchronously.

    Usage:
        @dual
        async def run():
            await ...

        # Both work:
        run()          # blocks in sync context
        await run()    # works in async context
    """
    if not inspect.iscoroutinefunction(func):
        raise TypeError("@dual can only be applied to async def functions")

    @functools.wraps(func)
    def sync_or_async_call(*args: Any, **kwargs: Any) -> Any:
        coro = func(*args, **kwargs)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        return coro if loop.is_running() else asyncio.run(coro)  # type: ignore[arg-type]

    sync_or_async_call = functools.update_wrapper(sync_or_async_call, func)
    return sync_or_async_call
