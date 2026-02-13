"""
Kafka message adapter.

Bridges Apache Kafka topics with Interlace streams.
Requires: pip install aiokafka

Example:
    from interlace.streaming import KafkaAdapter

    adapter = KafkaAdapter(
        bootstrap_servers="localhost:9092",
        group_id="interlace-consumers",
    )

    # Kafka topic -> Interlace stream
    async with adapter:
        await adapter.consume_to_stream("orders", "order_events")
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

from interlace.streaming.adapters.base import (
    AdapterConfig,
    DeserializationFormat,
    Message,
    MessageAdapter,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.streaming.kafka")


class KafkaAdapter(MessageAdapter):
    """
    Apache Kafka adapter for Interlace streams.

    Provides bidirectional bridging between Kafka topics and Interlace streams.

    Args:
        bootstrap_servers: Kafka broker addresses (comma-separated or list)
        group_id: Consumer group ID for load balancing
        client_id: Client identifier
        security_protocol: Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
        sasl_mechanism: SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
        sasl_username: SASL username
        sasl_password: SASL password
        ssl_context: SSL context for secure connections
        config: Adapter configuration
        **kafka_config: Additional aiokafka configuration
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str | None = None,
        client_id: str = "interlace",
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
        ssl_context: Any | None = None,
        config: AdapterConfig | None = None,
        **kafka_config: Any,
    ) -> None:
        super().__init__(config)
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.client_id = client_id
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.ssl_context = ssl_context
        self.kafka_config = kafka_config

        self._consumer = None
        self._producer = None

    async def connect(self) -> None:
        """Connect to Kafka."""
        try:
            from aiokafka import AIOKafkaProducer
        except ImportError as e:
            raise ImportError(
                "aiokafka is required for Kafka integration. " "Install it with: pip install aiokafka"
            ) from e

        common_config = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": self.client_id,
            "security_protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            common_config["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            common_config["sasl_plain_username"] = self.sasl_username
        if self.sasl_password:
            common_config["sasl_plain_password"] = self.sasl_password
        if self.ssl_context:
            common_config["ssl_context"] = self.ssl_context
        common_config.update(self.kafka_config)

        # Create producer
        self._producer = AIOKafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            **common_config,
        )
        await self._producer.start()  # type: ignore[attr-defined]
        logger.info(f"Connected Kafka producer to {self.bootstrap_servers}")

    async def disconnect(self) -> None:
        """Disconnect from Kafka."""
        if self._producer:
            await self._producer.stop()
            self._producer = None
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
        logger.info("Disconnected from Kafka")

    async def consume(  # type: ignore[override, misc]
        self,
        topic: str,
        *,
        group_id: str | None = None,
        from_beginning: bool = False,
    ) -> AsyncIterator[Message]:
        """Consume messages from a Kafka topic."""
        try:
            from aiokafka import AIOKafkaConsumer
        except ImportError as e:
            raise ImportError(
                "aiokafka is required for Kafka integration. " "Install it with: pip install aiokafka"
            ) from e

        consumer_group = group_id or self.group_id or f"interlace-{topic}"

        consumer_config = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": f"{self.client_id}-consumer",
            "group_id": consumer_group,
            "auto_offset_reset": "earliest" if from_beginning else "latest",
            "security_protocol": self.security_protocol,
            "enable_auto_commit": True,
        }
        if self.sasl_mechanism:
            consumer_config["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            consumer_config["sasl_plain_username"] = self.sasl_username
        if self.sasl_password:
            consumer_config["sasl_plain_password"] = self.sasl_password
        if self.ssl_context:
            consumer_config["ssl_context"] = self.ssl_context
        consumer_config.update(self.kafka_config)

        consumer = AIOKafkaConsumer(topic, **consumer_config)
        await consumer.start()
        self._consumer = consumer

        try:
            async for kafka_msg in consumer:
                value = self._deserialize(kafka_msg.value)

                headers = {}
                if kafka_msg.headers:
                    for k, v in kafka_msg.headers:
                        headers[k] = v.decode("utf-8") if isinstance(v, bytes) else str(v)

                yield Message(
                    key=kafka_msg.key.decode("utf-8") if kafka_msg.key else None,
                    value=value,
                    headers=headers,
                    timestamp=(
                        datetime.fromtimestamp(kafka_msg.timestamp / 1000, tz=UTC) if kafka_msg.timestamp else None
                    ),
                    topic=kafka_msg.topic,
                    partition=kafka_msg.partition,
                    offset=kafka_msg.offset,
                    metadata={"source": "kafka"},
                )
        finally:
            await consumer.stop()
            self._consumer = None

    async def produce(self, topic: str, message: Message) -> None:
        """Produce a message to a Kafka topic."""
        if not self._producer:
            raise RuntimeError("Kafka producer not connected. Call connect() first.")

        key = message.key.encode("utf-8") if message.key else None
        headers = [(k, v.encode("utf-8")) for k, v in message.headers.items()] if message.headers else None

        value = message.value if isinstance(message.value, dict) else {"payload": message.value}

        await self._producer.send_and_wait(
            topic,
            value=value,
            key=key,
            headers=headers,
        )

    async def produce_batch(self, topic: str, messages: list[Message]) -> int:
        """Produce a batch of messages to Kafka (uses batching)."""
        if not self._producer:
            raise RuntimeError("Kafka producer not connected. Call connect() first.")

        batch = self._producer.create_batch()
        count = 0

        for msg in messages:
            key = msg.key.encode("utf-8") if msg.key else None
            value = msg.value if isinstance(msg.value, dict) else {"payload": msg.value}
            value_bytes = json.dumps(value).encode("utf-8")

            metadata = batch.append(key=key, value=value_bytes, timestamp=None)
            if metadata is None:
                # Batch is full, send it
                await self._producer.send_and_wait(topic)
                batch = self._producer.create_batch()
                batch.append(key=key, value=value_bytes, timestamp=None)
            count += 1

        # Send remaining
        if count > 0:
            partitions = await self._producer.partitions_for(topic)
            partition = list(partitions)[0] if partitions else 0
            await self._producer.send_batch(batch, topic, partition=partition)

        return count

    def _deserialize(self, data: bytes) -> Any:
        """Deserialize message value based on configured format."""
        fmt = self.config.format

        if fmt == DeserializationFormat.JSON:
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return {"payload": data.decode("utf-8", errors="replace")}

        elif fmt == DeserializationFormat.MSGPACK:
            try:
                import msgpack

                return msgpack.unpackb(data, raw=False)
            except ImportError as e:
                raise ImportError("msgpack is required for MSGPACK deserialization") from e

        elif fmt == DeserializationFormat.AVRO:
            # Avro requires schema registry - pass through as bytes for now
            return {"payload_bytes": data.hex(), "_format": "avro"}

        elif fmt == DeserializationFormat.PROTOBUF:
            return {"payload_bytes": data.hex(), "_format": "protobuf"}

        else:  # RAW
            return {"payload": data.decode("utf-8", errors="replace")}
