"""
Redis Streams adapter.

Bridges Redis Streams with Interlace streams.
Requires: pip install redis[async] (redis >= 4.2.0 with async support)

Example:
    from interlace.streaming import RedisAdapter

    adapter = RedisAdapter(url="redis://localhost:6379")

    # Redis stream -> Interlace stream
    async with adapter:
        await adapter.consume_to_stream("orders-stream", "order_events")
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from interlace.streaming.adapters.base import (
    AdapterConfig,
    Message,
    MessageAdapter,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.streaming.redis")


class RedisAdapter(MessageAdapter):
    """
    Redis Streams adapter for Interlace.

    Uses Redis Streams (XADD/XREAD/XREADGROUP) for reliable message delivery
    with consumer group support.

    Args:
        url: Redis connection URL (redis://localhost:6379)
        host: Redis host (alternative to url)
        port: Redis port (default 6379)
        db: Redis database number
        password: Redis password
        ssl: Enable SSL
        config: Adapter configuration
    """

    def __init__(
        self,
        url: str | None = None,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        ssl: bool = False,
        config: AdapterConfig | None = None,
    ):
        super().__init__(config)
        self.url = url
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.ssl = ssl
        self._client = None

    async def connect(self) -> None:
        """Connect to Redis."""
        try:
            import redis.asyncio as aioredis
        except ImportError as e:
            raise ImportError(
                "redis with async support is required for Redis integration. " "Install it with: pip install redis"
            ) from e

        if self.url:
            self._client = aioredis.from_url(self.url, decode_responses=True)
        else:
            self._client = aioredis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                ssl=self.ssl,
                decode_responses=True,
            )

        # Test connection
        await self._client.ping()  # type: ignore[attr-defined]
        logger.info(f"Connected to Redis at {self.url or f'{self.host}:{self.port}'}")

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info("Disconnected from Redis")

    async def consume(  # type: ignore[override, misc]
        self,
        topic: str,
        *,
        group_id: str | None = None,
        from_beginning: bool = False,
    ) -> AsyncIterator[Message]:
        """Consume messages from a Redis stream."""
        if not self._client:
            raise RuntimeError("Redis not connected. Call connect() first.")

        if group_id:
            # Consumer group mode (reliable, at-least-once)
            consumer_name = f"interlace-{asyncio.current_task().get_name()}"

            # Create consumer group if it doesn't exist
            try:
                await self._client.xgroup_create(
                    topic,
                    group_id,
                    id="0" if from_beginning else "$",
                    mkstream=True,
                )
            except Exception:
                # Group already exists
                pass

            while self._running:
                try:
                    results = await self._client.xreadgroup(
                        group_id,
                        consumer_name,
                        {topic: ">"},
                        count=self.config.batch_size,
                        block=self.config.batch_timeout_ms,
                    )

                    for _stream_name, messages in results:
                        for msg_id, fields in messages:
                            yield self._redis_to_message(msg_id, fields, topic)

                            # Acknowledge message
                            await self._client.xack(topic, group_id, msg_id)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.warning(f"Redis consume error: {e}")
                    await asyncio.sleep(1.0)
        else:
            # Simple consumer (no group, no ACK)
            last_id = "0" if from_beginning else "$"

            while self._running:
                try:
                    results = await self._client.xread(
                        {topic: last_id},
                        count=self.config.batch_size,
                        block=self.config.batch_timeout_ms,
                    )

                    for _stream_name, messages in results:
                        for msg_id, fields in messages:
                            last_id = msg_id
                            yield self._redis_to_message(msg_id, fields, topic)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.warning(f"Redis consume error: {e}")
                    await asyncio.sleep(1.0)

    async def produce(self, topic: str, message: Message) -> None:
        """Produce a message to a Redis stream."""
        if not self._client:
            raise RuntimeError("Redis not connected. Call connect() first.")

        fields = {}
        if isinstance(message.value, dict):
            # Flatten dict to Redis hash fields
            for k, v in message.value.items():
                fields[k] = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
        else:
            fields["payload"] = str(message.value)

        if message.key:
            fields["_key"] = message.key

        await self._client.xadd(topic, fields)

    async def produce_batch(self, topic: str, messages: list[Message]) -> int:
        """Produce a batch of messages using Redis pipeline."""
        if not self._client:
            raise RuntimeError("Redis not connected. Call connect() first.")

        async with self._client.pipeline(transaction=False) as pipe:
            for msg in messages:
                fields = {}
                if isinstance(msg.value, dict):
                    for k, v in msg.value.items():
                        fields[k] = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                else:
                    fields["payload"] = str(msg.value)
                if msg.key:
                    fields["_key"] = msg.key
                pipe.xadd(topic, fields)

            results = await pipe.execute()
            return len(results)

    def _redis_to_message(self, msg_id: str, fields: dict[str, str], topic: str) -> Message:
        """Convert Redis stream entry to Message."""
        # Try to parse JSON values
        parsed = {}
        for k, v in fields.items():
            if k.startswith("_"):
                continue
            try:
                parsed[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                parsed[k] = v

        # Extract timestamp from Redis message ID (milliseconds since epoch)
        ts = None
        try:
            ts_ms = int(msg_id.split("-")[0])
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
        except (ValueError, IndexError):
            pass

        return Message(
            key=fields.get("_key"),
            value=parsed,
            timestamp=ts,
            topic=topic,
            offset=None,
            metadata={"source": "redis", "redis_id": msg_id},
        )
