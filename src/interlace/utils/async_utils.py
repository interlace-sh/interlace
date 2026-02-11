"""
Async utilities for Interlace.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from typing import Any, Awaitable, Callable, TypeVar, overload

T = TypeVar("T")


def dual(func: Callable[..., Awaitable[T]]) -> Callable[..., T]:
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

    @overload
    def sync_or_async_call(*args: Any, **kwargs: Any) -> T: ...

    @overload
    async def sync_or_async_call(*args: Any, **kwargs: Any) -> T: ...

    @functools.wraps(func)
    def sync_or_async_call(*args: Any, **kwargs: Any) -> Any:
        coro = func(*args, **kwargs)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        return coro if loop.is_running() else asyncio.run(coro)

    sync_or_async_call = functools.update_wrapper(sync_or_async_call, func)  # type: ignore
    return sync_or_async_call  # type: ignore
