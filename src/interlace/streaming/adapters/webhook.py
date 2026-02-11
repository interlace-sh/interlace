"""
Outbound webhook adapter.

Sends Interlace stream events to external HTTP endpoints as webhooks.

Example:
    from interlace.streaming import WebhookAdapter

    adapter = WebhookAdapter(
        endpoints={
            "order_events": "https://api.example.com/webhooks/orders",
            "user_events": "https://api.example.com/webhooks/users",
        },
        headers={"Authorization": "Bearer token123"},
    )

    # Stream events -> HTTP webhooks
    async with adapter:
        await adapter.stream_to_produce("order_events", "order_events")
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, List, Optional

from interlace.streaming.adapters.base import (
    AdapterConfig,
    Message,
    MessageAdapter,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.streaming.webhook")


class WebhookAdapter(MessageAdapter):
    """
    Outbound webhook adapter.

    Sends stream events as HTTP POST requests to configured endpoints.
    Supports:
    - HMAC signature verification (for receivers to verify authenticity)
    - Retry with exponential backoff
    - Batch mode (send multiple events in one request)
    - Custom headers and authentication

    Args:
        endpoints: Mapping of topic/stream names to webhook URLs
        headers: Default headers for all requests
        signing_secret: HMAC signing secret for webhook signatures
        timeout: Request timeout in seconds
        retry_count: Number of retries on failure
        batch_mode: If True, batches events into array payloads
        config: Adapter configuration
    """

    def __init__(
        self,
        endpoints: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        signing_secret: Optional[str] = None,
        timeout: float = 30.0,
        retry_count: int = 3,
        batch_mode: bool = False,
        config: Optional[AdapterConfig] = None,
    ):
        super().__init__(config)
        self.endpoints = endpoints or {}
        self.default_headers = headers or {}
        self.signing_secret = signing_secret
        self.timeout = timeout
        self.retry_count = retry_count
        self.batch_mode = batch_mode
        self._session = None

    async def connect(self) -> None:
        """Initialize HTTP session."""
        try:
            import aiohttp
        except ImportError:
            raise ImportError(
                "aiohttp is required for webhook integration. "
                "Install it with: pip install aiohttp"
            )

        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self._session = aiohttp.ClientSession(
            headers=self.default_headers,
            timeout=timeout,
        )
        logger.info(f"Webhook adapter initialized with {len(self.endpoints)} endpoint(s)")

    async def disconnect(self) -> None:
        """Close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None

    async def consume(
        self,
        topic: str,
        *,
        group_id: Optional[str] = None,
        from_beginning: bool = False,
    ) -> AsyncIterator[Message]:
        """
        Webhooks are outbound-only.

        For inbound webhooks, use the stream HTTP endpoint directly:
        POST /api/v1/streams/{stream_name}
        """
        raise NotImplementedError(
            "WebhookAdapter is outbound-only. "
            "For inbound webhooks, use POST /api/v1/streams/{stream_name}"
        )
        yield  # Make this a generator (unreachable)

    async def produce(self, topic: str, message: Message) -> None:
        """Send a webhook to the configured endpoint."""
        url = self.endpoints.get(topic)
        if not url:
            logger.warning(f"No webhook endpoint configured for topic '{topic}'")
            return

        payload = message.value if isinstance(message.value, dict) else {"payload": message.value}

        await self._send_webhook(url, payload, topic)

    async def produce_batch(self, topic: str, messages: List[Message]) -> int:
        """Send webhooks for a batch of messages."""
        url = self.endpoints.get(topic)
        if not url:
            logger.warning(f"No webhook endpoint configured for topic '{topic}'")
            return 0

        if self.batch_mode:
            # Send all as single array payload
            payloads = [
                m.value if isinstance(m.value, dict) else {"payload": m.value}
                for m in messages
            ]
            await self._send_webhook(url, payloads, topic)
            return len(messages)
        else:
            count = 0
            for msg in messages:
                payload = msg.value if isinstance(msg.value, dict) else {"payload": msg.value}
                await self._send_webhook(url, payload, topic)
                count += 1
            return count

    async def _send_webhook(
        self,
        url: str,
        payload: Any,
        topic: str,
    ) -> None:
        """Send a webhook with retry logic."""
        if not self._session:
            raise RuntimeError("Webhook adapter not connected. Call connect() first.")

        body = json.dumps(payload, default=str).encode("utf-8")

        headers = {
            "Content-Type": "application/json",
            "X-Interlace-Topic": topic,
            "X-Interlace-Timestamp": str(int(time.time())),
        }

        # Add HMAC signature if configured
        if self.signing_secret:
            timestamp = headers["X-Interlace-Timestamp"]
            signature_payload = f"{timestamp}.{body.decode('utf-8')}"
            signature = hmac.new(
                self.signing_secret.encode("utf-8"),
                signature_payload.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            headers["X-Interlace-Signature"] = f"sha256={signature}"

        # Retry loop
        last_error = None
        for attempt in range(self.retry_count + 1):
            try:
                async with self._session.post(url, data=body, headers=headers) as resp:
                    if resp.status < 300:
                        logger.debug(
                            f"Webhook sent to {url} (status={resp.status}, "
                            f"topic={topic})"
                        )
                        return
                    elif resp.status >= 500:
                        # Server error - retry
                        last_error = f"HTTP {resp.status}"
                        logger.warning(
                            f"Webhook to {url} failed (status={resp.status}), "
                            f"attempt {attempt + 1}/{self.retry_count + 1}"
                        )
                    else:
                        # Client error - don't retry
                        response_text = await resp.text()
                        logger.error(
                            f"Webhook to {url} rejected (status={resp.status}): "
                            f"{response_text[:200]}"
                        )
                        return

            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"Webhook to {url} error: {e}, "
                    f"attempt {attempt + 1}/{self.retry_count + 1}"
                )

            # Exponential backoff
            if attempt < self.retry_count:
                delay = min(2 ** attempt, 30)
                await asyncio.sleep(delay)

        logger.error(
            f"Webhook to {url} failed after {self.retry_count + 1} attempts: {last_error}"
        )
