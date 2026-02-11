"""
Base message adapter interface.

All messaging system adapters (Kafka, RabbitMQ, Redis, etc.) implement this
interface to provide a unified way to bridge external message queues with
Interlace streams.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Union


class DeserializationFormat(str, Enum):
    """Supported message deserialization formats."""
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"
    MSGPACK = "msgpack"
    RAW = "raw"  # Pass through as {"payload": raw_bytes}


@dataclass
class Message:
    """
    A message from an external messaging system.

    Provides a unified representation regardless of source (Kafka, RabbitMQ, etc.)
    """
    key: Optional[str] = None
    value: Any = None
    headers: Dict[str, str] = field(default_factory=dict)
    timestamp: Optional[datetime] = None
    topic: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dict suitable for stream ingestion."""
        result = {}

        if isinstance(self.value, dict):
            result.update(self.value)
        else:
            result["payload"] = self.value

        if self.key is not None:
            result.setdefault("_message_key", self.key)
        if self.topic is not None:
            result.setdefault("_source_topic", self.topic)
        if self.timestamp is not None:
            result.setdefault("_message_timestamp", self.timestamp.isoformat())

        return result


@dataclass
class AdapterConfig:
    """
    Configuration for a message adapter.

    Provides common config that all adapters support.
    """
    # Deserialization
    format: DeserializationFormat = DeserializationFormat.JSON

    # Batching
    batch_size: int = 100
    batch_timeout_ms: int = 1000

    # Error handling
    max_retries: int = 3
    retry_delay_ms: int = 1000
    dead_letter_topic: Optional[str] = None

    # Transform
    transform_fn: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None

    # Rate limiting
    max_messages_per_second: Optional[int] = None

    # Additional adapter-specific config
    extra: Dict[str, Any] = field(default_factory=dict)


class MessageAdapter(ABC):
    """
    Abstract base class for message queue adapters.

    Adapters provide bidirectional bridging between external messaging systems
    and Interlace streams:

    - consume_to_stream(): External queue -> Interlace stream
    - stream_to_produce(): Interlace stream -> External queue
    - consume(): Raw async iterator over messages

    Implementations provided:
    - KafkaAdapter: Apache Kafka (requires aiokafka)
    - RedisAdapter: Redis Streams/Pub-Sub (requires aioredis)
    - RabbitMQAdapter: RabbitMQ/AMQP (requires aio-pika)
    - WebhookAdapter: Outbound HTTP webhooks
    - InMemoryAdapter: In-process testing adapter

    Example:
        adapter = KafkaAdapter(bootstrap_servers="localhost:9092")

        # External -> Interlace
        task = asyncio.create_task(
            adapter.consume_to_stream("orders-topic", "order_events")
        )

        # Interlace -> External
        task2 = asyncio.create_task(
            adapter.stream_to_produce("processed_orders", "processed-topic")
        )
    """

    def __init__(self, config: Optional[AdapterConfig] = None):
        self.config = config or AdapterConfig()
        self._running = False
        self._tasks: List[asyncio.Task] = []

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the messaging system."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully disconnect from the messaging system."""
        ...

    @abstractmethod
    async def consume(
        self,
        topic: str,
        *,
        group_id: Optional[str] = None,
        from_beginning: bool = False,
    ) -> AsyncIterator[Message]:
        """
        Consume messages from an external topic/queue.

        Args:
            topic: Topic/queue/channel name
            group_id: Consumer group ID (for load balancing)
            from_beginning: Start from earliest message

        Yields:
            Message objects
        """
        ...

    @abstractmethod
    async def produce(
        self,
        topic: str,
        message: Message,
    ) -> None:
        """
        Produce a message to an external topic/queue.

        Args:
            topic: Target topic/queue/channel name
            message: Message to send
        """
        ...

    async def produce_batch(
        self,
        topic: str,
        messages: List[Message],
    ) -> int:
        """
        Produce a batch of messages.

        Default implementation sends one at a time.
        Subclasses should override for efficient batching.

        Returns:
            Number of messages sent
        """
        count = 0
        for msg in messages:
            await self.produce(topic, msg)
            count += 1
        return count

    async def consume_to_stream(
        self,
        topic: str,
        stream_name: str,
        *,
        group_id: Optional[str] = None,
        from_beginning: bool = False,
        config: Optional[AdapterConfig] = None,
    ) -> None:
        """
        Bridge: consume from external topic and publish to Interlace stream.

        Runs continuously until stopped or an unrecoverable error occurs.

        Args:
            topic: External topic/queue to consume from
            stream_name: Interlace stream to publish to
            group_id: Consumer group for load balancing
            from_beginning: Start from earliest message
            config: Override adapter config for this bridge
        """
        from interlace.core.stream import publish

        cfg = config or self.config
        batch: List[Dict[str, Any]] = []
        last_flush = asyncio.get_event_loop().time()

        self._running = True

        async for msg in self.consume(topic, group_id=group_id, from_beginning=from_beginning):
            if not self._running:
                break

            event = msg.to_dict()

            # Apply transform
            if cfg.transform_fn:
                try:
                    event = cfg.transform_fn(event)
                except Exception as e:
                    from interlace.utils.logging import get_logger
                    get_logger("interlace.streaming").warning(
                        f"Transform error for message from '{topic}': {e}"
                    )
                    continue

            # Apply filter
            if cfg.filter_fn and not cfg.filter_fn(event):
                continue

            batch.append(event)

            # Flush conditions
            now = asyncio.get_event_loop().time()
            time_elapsed_ms = (now - last_flush) * 1000
            should_flush = (
                len(batch) >= cfg.batch_size
                or time_elapsed_ms >= cfg.batch_timeout_ms
            )

            if should_flush and batch:
                await publish(stream_name, batch)
                batch = []
                last_flush = now

                # Rate limiting
                if cfg.max_messages_per_second:
                    await asyncio.sleep(1.0 / cfg.max_messages_per_second)

        # Flush remaining
        if batch:
            await publish(stream_name, batch)

    async def stream_to_produce(
        self,
        stream_name: str,
        topic: str,
        *,
        config: Optional[AdapterConfig] = None,
    ) -> None:
        """
        Bridge: subscribe to Interlace stream and produce to external topic.

        Runs continuously until stopped.

        Args:
            stream_name: Interlace stream to subscribe to
            topic: External topic/queue to produce to
            config: Override adapter config
        """
        from interlace.core.stream import subscribe

        cfg = config or self.config
        self._running = True

        async for event in subscribe(stream_name, timeout=5.0):
            if not self._running:
                break

            if isinstance(event, list):
                messages = [
                    Message(
                        value=e,
                        topic=topic,
                        timestamp=datetime.now(timezone.utc),
                    )
                    for e in event
                ]
                await self.produce_batch(topic, messages)
            else:
                msg = Message(
                    value=event,
                    topic=topic,
                    timestamp=datetime.now(timezone.utc),
                )
                await self.produce(topic, msg)

    async def stop(self) -> None:
        """Stop all running bridges."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc):
        await self.stop()
        await self.disconnect()

    @property
    def is_running(self) -> bool:
        return self._running
