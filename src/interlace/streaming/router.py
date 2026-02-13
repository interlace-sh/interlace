"""
Stream router - declarative routing for stream events.

Provides a Flask/FastAPI-like routing interface for defining how
stream events should be processed, transformed, and forwarded.

Example:
    from interlace.streaming import StreamRouter

    router = StreamRouter()

    @router.on("user_events")
    async def handle_user_event(event):
        print(f"User event: {event}")

    @router.on("user_events", filter=lambda e: e.get("type") == "signup")
    async def handle_signup(event):
        await send_welcome_email(event["email"])

    @router.forward("order_events", to="analytics_orders", transform=enrich_order)

    # Run the router
    await router.start()
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from interlace.utils.logging import get_logger

logger = get_logger("interlace.streaming.router")


@dataclass
class Route:
    """A routing rule for stream events."""

    stream_name: str
    handler: Callable | None = None
    filter_fn: Callable[[dict[str, Any]], bool] | None = None
    transform_fn: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    forward_to: str | None = None
    error_handler: Callable | None = None
    name: str | None = None


class StreamRouter:
    """
    Declarative event router for Interlace streams.

    Provides a decorator-based API for defining how stream events
    should be processed, similar to web framework routing.

    Features:
    - Event handlers with filtering
    - Event forwarding between streams
    - Transform pipelines
    - Error handling per route
    - Concurrent processing

    Example:
        router = StreamRouter()

        @router.on("orders")
        async def process_order(event):
            # Process each order event
            await update_inventory(event)

        @router.on("orders", filter=lambda e: e["total"] > 1000)
        async def flag_large_order(event):
            await notify_sales_team(event)

        # Forward with transformation
        router.forward(
            "raw_events",
            to="cleaned_events",
            transform=lambda e: {k: v for k, v in e.items() if not k.startswith("_")}
        )

        await router.start()
    """

    def __init__(self, concurrency: int = 10):
        self._routes: list[Route] = []
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._semaphore = asyncio.Semaphore(concurrency)

    def on(
        self,
        stream_name: str,
        *,
        filter: Callable[[dict[str, Any]], bool] | None = None,
        name: str | None = None,
        error_handler: Callable | None = None,
    ) -> Callable:
        """
        Decorator to register an event handler for a stream.

        Args:
            stream_name: Stream to listen to
            filter: Optional filter function
            name: Optional route name for debugging
            error_handler: Optional error handler

        Example:
            @router.on("user_events")
            async def handle(event):
                print(event)
        """

        def decorator(func: Callable) -> Callable:
            self._routes.append(
                Route(
                    stream_name=stream_name,
                    handler=func,
                    filter_fn=filter,
                    name=name or func.__name__,
                    error_handler=error_handler,
                )
            )
            return func

        return decorator

    def forward(
        self,
        stream_name: str,
        *,
        to: str,
        transform: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
        filter: Callable[[dict[str, Any]], bool] | None = None,
        name: str | None = None,
    ) -> None:
        """
        Forward events from one stream to another, optionally transforming them.

        Args:
            stream_name: Source stream
            to: Target stream name
            transform: Optional transformation function
            filter: Optional filter function
            name: Optional route name

        Example:
            router.forward("raw_events", to="cleaned_events",
                           transform=lambda e: clean(e))
        """
        self._routes.append(
            Route(
                stream_name=stream_name,
                forward_to=to,
                transform_fn=transform,
                filter_fn=filter,
                name=name or f"forward-{stream_name}->{to}",
            )
        )

    async def start(self) -> None:
        """Start processing all routes."""
        self._running = True

        # Group routes by stream name
        streams: dict[str, list[Any]] = {}
        for route in self._routes:
            if route.stream_name not in streams:
                streams[route.stream_name] = []
            streams[route.stream_name].append(route)

        # Create one subscription task per stream
        for stream_name, routes in streams.items():
            task = asyncio.create_task(
                self._process_stream(stream_name, routes),
                name=f"router-{stream_name}",
            )
            self._tasks.append(task)

        logger.info(f"StreamRouter started with {len(self._routes)} route(s) " f"across {len(streams)} stream(s)")

    async def stop(self) -> None:
        """Stop processing."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _process_stream(
        self,
        stream_name: str,
        routes: list[Route],
    ) -> None:
        """Process events from a stream and dispatch to routes."""
        from interlace.core.stream import subscribe

        async for event in subscribe(stream_name, timeout=5.0):
            if not self._running:
                break

            # Dispatch to all matching routes concurrently
            tasks = []
            for route in routes:
                # Check filter
                if route.filter_fn:
                    try:
                        if not route.filter_fn(event):  # type: ignore[arg-type]
                            continue
                    except Exception:
                        continue

                tasks.append(self._dispatch(route, event))  # type: ignore[arg-type]

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _dispatch(self, route: Route, event: dict[str, Any]) -> None:
        """Dispatch a single event to a route."""
        async with self._semaphore:
            try:
                if route.handler:
                    # Call handler
                    result = route.handler(event)
                    if asyncio.iscoroutine(result):
                        await result

                if route.forward_to:
                    # Forward to another stream
                    from interlace.core.stream import publish

                    forwarded_event = event
                    if route.transform_fn:
                        forwarded_event = route.transform_fn(event)

                    await publish(route.forward_to, forwarded_event)

            except Exception as e:
                if route.error_handler:
                    try:
                        result = route.error_handler(event, e)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as handler_err:
                        logger.error(f"Error handler for route '{route.name}' failed: {handler_err}")
                else:
                    logger.error(f"Route '{route.name}' failed for event: {e}")

    @property
    def routes(self) -> list[Route]:
        return list(self._routes)

    @property
    def is_running(self) -> bool:
        return self._running

    async def __aenter__(self) -> StreamRouter:
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()
