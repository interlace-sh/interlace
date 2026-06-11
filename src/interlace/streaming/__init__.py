"""
Streaming module for Interlace.

Provides message adapters and streaming utilities for integrating
Interlace streams with external systems via webhooks, polling, and pub/sub.

Usage:
    from interlace.streaming import WebhookAdapter, StreamBridge

    adapter = WebhookAdapter(url="https://example.com/webhook")
    bridge = StreamBridge()
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
