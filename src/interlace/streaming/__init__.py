"""
Streaming module for Interlace.

Provides message queue adapters and streaming utilities for integrating
Interlace streams with external messaging systems (Kafka, RabbitMQ, Redis, etc.).

Usage:
    from interlace.streaming import KafkaAdapter, RedisAdapter, WebhookAdapter

    # Consume from Kafka -> publish to Interlace stream
    adapter = KafkaAdapter(bootstrap_servers="localhost:9092")
    await adapter.consume_to_stream("kafka-topic", "interlace_stream")

    # Subscribe to Interlace stream -> produce to Kafka
    await adapter.stream_to_produce("interlace_stream", "kafka-topic")
"""

from interlace.streaming.adapters.base import (
    AdapterConfig,
    Message,
    MessageAdapter,
)
from interlace.streaming.adapters.memory import InMemoryAdapter
from interlace.streaming.adapters.webhook import WebhookAdapter
from interlace.streaming.bridge import StreamBridge
from interlace.streaming.router import StreamRouter

__all__ = [
    "MessageAdapter",
    "Message",
    "AdapterConfig",
    "WebhookAdapter",
    "InMemoryAdapter",
    "StreamBridge",
    "StreamRouter",
]
