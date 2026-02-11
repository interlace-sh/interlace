"""
Stream bridge - connects external message systems to Interlace streams.

Provides a high-level API for setting up bidirectional bridges between
messaging systems and Interlace streams with lifecycle management.

Example:
    from interlace.streaming import StreamBridge, KafkaAdapter, WebhookAdapter

    bridge = StreamBridge()

    # Kafka -> Interlace
    bridge.add_inbound(
        KafkaAdapter(bootstrap_servers="localhost:9092"),
        source_topic="orders",
        target_stream="order_events",
    )

    # Interlace -> Webhook
    bridge.add_outbound(
        WebhookAdapter(endpoints={"order_events": "https://api.example.com/orders"}),
        source_stream="processed_orders",
        target_topic="order_events",
    )

    async with bridge:
        await bridge.run_forever()
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from interlace.streaming.adapters.base import AdapterConfig, MessageAdapter
from interlace.utils.logging import get_logger

logger = get_logger("interlace.streaming.bridge")


@dataclass
class BridgeRoute:
    """A route connecting a message adapter to/from an Interlace stream."""
    adapter: MessageAdapter
    source: str
    target: str
    direction: str  # "inbound" or "outbound"
    config: Optional[AdapterConfig] = None
    group_id: Optional[str] = None
    from_beginning: bool = False


class StreamBridge:
    """
    High-level bridge manager connecting message systems to Interlace streams.

    Manages the lifecycle of multiple adapter connections and bridges,
    handling startup, shutdown, and error recovery.

    Example:
        bridge = StreamBridge()

        # Add routes
        bridge.add_inbound(kafka, "orders-topic", "order_events")
        bridge.add_outbound(webhook, "alerts", "alerts-topic")

        # Run
        async with bridge:
            await bridge.run_forever()
    """

    def __init__(self):
        self._routes: List[BridgeRoute] = []
        self._tasks: List[asyncio.Task] = []
        self._running = False
        self._adapters: List[MessageAdapter] = []

    def add_inbound(
        self,
        adapter: MessageAdapter,
        source_topic: str,
        target_stream: str,
        *,
        group_id: Optional[str] = None,
        from_beginning: bool = False,
        config: Optional[AdapterConfig] = None,
    ) -> "StreamBridge":
        """
        Add an inbound route: external topic -> Interlace stream.

        Args:
            adapter: Message adapter instance
            source_topic: External topic/queue to consume from
            target_stream: Interlace stream to publish to
            group_id: Consumer group for load balancing
            from_beginning: Start from earliest message
            config: Override adapter config

        Returns:
            self (for chaining)
        """
        self._routes.append(BridgeRoute(
            adapter=adapter,
            source=source_topic,
            target=target_stream,
            direction="inbound",
            config=config,
            group_id=group_id,
            from_beginning=from_beginning,
        ))
        if adapter not in self._adapters:
            self._adapters.append(adapter)
        return self

    def add_outbound(
        self,
        adapter: MessageAdapter,
        source_stream: str,
        target_topic: str,
        *,
        config: Optional[AdapterConfig] = None,
    ) -> "StreamBridge":
        """
        Add an outbound route: Interlace stream -> external topic.

        Args:
            adapter: Message adapter instance
            source_stream: Interlace stream to subscribe to
            target_topic: External topic/queue to produce to
            config: Override adapter config

        Returns:
            self (for chaining)
        """
        self._routes.append(BridgeRoute(
            adapter=adapter,
            source=source_stream,
            target=target_topic,
            direction="outbound",
            config=config,
        ))
        if adapter not in self._adapters:
            self._adapters.append(adapter)
        return self

    async def start(self) -> None:
        """Connect all adapters and start bridge tasks."""
        self._running = True

        # Connect all unique adapters
        for adapter in self._adapters:
            await adapter.connect()

        # Start bridge tasks
        for route in self._routes:
            if route.direction == "inbound":
                task = asyncio.create_task(
                    self._run_inbound(route),
                    name=f"bridge-in-{route.source}->{route.target}",
                )
            else:
                task = asyncio.create_task(
                    self._run_outbound(route),
                    name=f"bridge-out-{route.source}->{route.target}",
                )
            self._tasks.append(task)

        logger.info(
            f"StreamBridge started with {len(self._routes)} route(s) "
            f"({sum(1 for r in self._routes if r.direction == 'inbound')} inbound, "
            f"{sum(1 for r in self._routes if r.direction == 'outbound')} outbound)"
        )

    async def stop(self) -> None:
        """Stop all bridge tasks and disconnect adapters."""
        self._running = False

        # Stop adapters
        for adapter in self._adapters:
            await adapter.stop()

        # Cancel tasks
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # Disconnect adapters
        for adapter in self._adapters:
            await adapter.disconnect()

        logger.info("StreamBridge stopped")

    async def run_forever(self) -> None:
        """Run until stopped or all tasks complete/fail."""
        if not self._tasks:
            await self.start()

        try:
            # Wait for all tasks (they run forever until stopped)
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass

    async def _run_inbound(self, route: BridgeRoute) -> None:
        """Run an inbound bridge with error recovery."""
        while self._running:
            try:
                await route.adapter.consume_to_stream(
                    route.source,
                    route.target,
                    group_id=route.group_id,
                    from_beginning=route.from_beginning,
                    config=route.config,
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Inbound bridge {route.source} -> {route.target} error: {e}. "
                    f"Restarting in 5s..."
                )
                await asyncio.sleep(5.0)

    async def _run_outbound(self, route: BridgeRoute) -> None:
        """Run an outbound bridge with error recovery."""
        while self._running:
            try:
                await route.adapter.stream_to_produce(
                    route.source,
                    route.target,
                    config=route.config,
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Outbound bridge {route.source} -> {route.target} error: {e}. "
                    f"Restarting in 5s..."
                )
                await asyncio.sleep(5.0)

    @property
    def routes(self) -> List[BridgeRoute]:
        """Get all configured routes."""
        return list(self._routes)

    @property
    def is_running(self) -> bool:
        return self._running

    def status(self) -> Dict[str, Any]:
        """Get bridge status."""
        return {
            "running": self._running,
            "routes": len(self._routes),
            "inbound": sum(1 for r in self._routes if r.direction == "inbound"),
            "outbound": sum(1 for r in self._routes if r.direction == "outbound"),
            "adapters": len(self._adapters),
            "tasks": len(self._tasks),
        }

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *exc):
        await self.stop()
