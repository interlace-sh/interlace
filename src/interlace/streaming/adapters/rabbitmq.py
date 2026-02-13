"""
RabbitMQ/AMQP adapter.

Bridges RabbitMQ queues with Interlace streams.
Requires: pip install aio-pika

Example:
    from interlace.streaming import RabbitMQAdapter

    adapter = RabbitMQAdapter(url="amqp://guest:guest@localhost/")

    async with adapter:
        await adapter.consume_to_stream("orders-queue", "order_events")
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

from interlace.streaming.adapters.base import (
    AdapterConfig,
    Message,
    MessageAdapter,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.streaming.rabbitmq")


class RabbitMQAdapter(MessageAdapter):
    """
    RabbitMQ/AMQP adapter for Interlace.

    Uses aio-pika for async AMQP communication with RabbitMQ.

    Args:
        url: AMQP connection URL (amqp://guest:guest@localhost/)
        host: RabbitMQ host (alternative to url)
        port: RabbitMQ port (default 5672)
        login: Username
        password: Password
        virtualhost: Virtual host
        exchange: Default exchange name (empty = default exchange)
        config: Adapter configuration
    """

    def __init__(
        self,
        url: str | None = None,
        host: str = "localhost",
        port: int = 5672,
        login: str = "guest",
        password: str = "guest",
        virtualhost: str = "/",
        exchange: str = "",
        config: AdapterConfig | None = None,
    ):
        super().__init__(config)
        self.url = url or f"amqp://{login}:{password}@{host}:{port}/{virtualhost}"
        self.exchange_name = exchange
        self._connection = None
        self._channel = None
        self._exchange = None

    async def connect(self) -> None:
        """Connect to RabbitMQ."""
        try:
            import aio_pika
        except ImportError as e:
            raise ImportError(
                "aio-pika is required for RabbitMQ integration. " "Install it with: pip install aio-pika"
            ) from e

        self._connection = await aio_pika.connect_robust(self.url)
        self._channel = await self._connection.channel()  # type: ignore[attr-defined]

        # Set prefetch count for consumer
        await self._channel.set_qos(prefetch_count=self.config.batch_size)  # type: ignore[attr-defined]

        if self.exchange_name:
            self._exchange = await self._channel.declare_exchange(  # type: ignore[attr-defined]
                self.exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
        else:
            self._exchange = self._channel.default_exchange  # type: ignore[attr-defined]

        logger.info(f"Connected to RabbitMQ at {self.url.split('@')[-1]}")

    async def disconnect(self) -> None:
        """Disconnect from RabbitMQ."""
        if self._channel:
            await self._channel.close()
            self._channel = None
        if self._connection:
            await self._connection.close()
            self._connection = None
        logger.info("Disconnected from RabbitMQ")

    async def consume(  # type: ignore[override, misc]
        self,
        topic: str,
        *,
        group_id: str | None = None,
        from_beginning: bool = False,
    ) -> AsyncIterator[Message]:
        """Consume messages from a RabbitMQ queue."""
        import importlib.util

        if importlib.util.find_spec("aio_pika") is None:
            raise ImportError("aio-pika is required for RabbitMQ integration. " "Install it with: pip install aio-pika")

        if not self._channel:
            raise RuntimeError("RabbitMQ not connected. Call connect() first.")

        # Declare queue
        queue = await self._channel.declare_queue(
            topic,
            durable=True,
            auto_delete=False,
        )

        # Bind to exchange if needed
        if self.exchange_name and self._exchange:
            await queue.bind(self._exchange, routing_key=topic)

        self._running = True

        async with queue.iterator() as queue_iter:
            async for amqp_msg in queue_iter:
                if not self._running:
                    break

                async with amqp_msg.process():
                    value = self._deserialize_body(amqp_msg.body)

                    headers = {}
                    if amqp_msg.headers:
                        headers = {str(k): str(v) for k, v in amqp_msg.headers.items()}

                    yield Message(
                        key=amqp_msg.routing_key,
                        value=value,
                        headers=headers,
                        timestamp=(amqp_msg.timestamp if amqp_msg.timestamp else datetime.now(UTC)),
                        topic=topic,
                        metadata={
                            "source": "rabbitmq",
                            "message_id": amqp_msg.message_id,
                            "correlation_id": amqp_msg.correlation_id,
                            "delivery_tag": amqp_msg.delivery_tag,
                        },
                    )

    async def produce(self, topic: str, message: Message) -> None:
        """Produce a message to a RabbitMQ queue/exchange."""
        try:
            import aio_pika
        except ImportError as e:
            raise ImportError(
                "aio-pika is required for RabbitMQ integration. " "Install it with: pip install aio-pika"
            ) from e

        if not self._exchange:
            raise RuntimeError("RabbitMQ not connected. Call connect() first.")

        body = json.dumps(message.value if isinstance(message.value, dict) else {"payload": message.value}).encode(
            "utf-8"
        )

        amqp_msg = aio_pika.Message(
            body=body,
            content_type="application/json",
            headers=message.headers or {},
            timestamp=message.timestamp or datetime.now(UTC),
        )

        await self._exchange.publish(
            amqp_msg,
            routing_key=message.key or topic,
        )

    def _deserialize_body(self, body: bytes) -> Any:
        """Deserialize message body."""
        try:
            return json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {"payload": body.decode("utf-8", errors="replace")}
