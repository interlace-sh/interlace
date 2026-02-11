"""
In-memory message adapter for testing.

Provides a simple in-process message bus useful for testing
stream integrations without external dependencies.

Example:
    from interlace.streaming import InMemoryAdapter

    adapter = InMemoryAdapter()

    async with adapter:
        # Produce a message
        await adapter.produce("test-topic", Message(value={"key": "value"}))

        # Consume messages
        async for msg in adapter.consume("test-topic"):
            print(msg.value)
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional

from interlace.streaming.adapters.base import (
    AdapterConfig,
    Message,
    MessageAdapter,
)


class InMemoryAdapter(MessageAdapter):
    """
    In-memory message adapter for testing and development.

    Messages are stored in-process and can be consumed by other
    coroutines in the same event loop.

    Features:
    - No external dependencies
    - Supports consumer groups (round-robin distribution)
    - Message persistence within process lifetime
    - Perfect for unit/integration tests
    """

    def __init__(self, config: Optional[AdapterConfig] = None):
        super().__init__(config)
        self._topics: Dict[str, List[Message]] = defaultdict(list)
        self._queues: Dict[str, Dict[str, asyncio.Queue]] = defaultdict(dict)
        self._connected = False

    async def connect(self) -> None:
        """No-op for in-memory adapter."""
        self._connected = True

    async def disconnect(self) -> None:
        """Clear all topics and queues."""
        self._topics.clear()
        self._queues.clear()
        self._connected = False

    async def consume(
        self,
        topic: str,
        *,
        group_id: Optional[str] = None,
        from_beginning: bool = False,
    ) -> AsyncIterator[Message]:
        """Consume messages from in-memory topic."""
        consumer_id = group_id or f"consumer-{id(asyncio.current_task())}"
        queue: asyncio.Queue = asyncio.Queue()

        # Register consumer queue
        self._queues[topic][consumer_id] = queue

        # Replay existing messages if from_beginning
        if from_beginning:
            for msg in self._topics[topic]:
                await queue.put(msg)

        self._running = True

        try:
            while self._running:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield msg
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
        finally:
            self._queues[topic].pop(consumer_id, None)

    async def produce(self, topic: str, message: Message) -> None:
        """Produce a message to in-memory topic."""
        message.topic = topic
        if message.timestamp is None:
            message.timestamp = datetime.now(timezone.utc)
        message.offset = len(self._topics[topic])

        # Store message
        self._topics[topic].append(message)

        # Deliver to consumers
        for consumer_id, queue in self._queues.get(topic, {}).items():
            try:
                await queue.put(message)
            except Exception:
                pass

    async def produce_batch(self, topic: str, messages: List[Message]) -> int:
        """Produce a batch of messages."""
        for msg in messages:
            await self.produce(topic, msg)
        return len(messages)

    def get_topic_messages(self, topic: str) -> List[Message]:
        """Get all messages in a topic (for testing)."""
        return list(self._topics.get(topic, []))

    def clear_topic(self, topic: str) -> None:
        """Clear all messages in a topic (for testing)."""
        self._topics[topic] = []

    @property
    def topic_count(self) -> int:
        """Number of topics with messages."""
        return len(self._topics)
