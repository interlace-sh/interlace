"""
Event bus for real-time updates via Server-Sent Events (SSE).

Provides pub/sub mechanism for broadcasting execution events to connected clients.
"""

import asyncio
import json
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from interlace.utils.logging import get_logger

logger = get_logger("interlace.api.events")


class EventBus:
    """
    Event bus for broadcasting real-time updates.

    Supports:
    - Multiple subscribers per event type
    - Wildcard subscription (all events)
    - Flow-specific filtering
    - Automatic cleanup on disconnect

    Event Types:
    - flow.started, flow.completed, flow.failed, flow.cancelled
    - task.enqueued, task.waiting, task.ready, task.running
    - task.materialising, task.completed, task.failed, task.skipped
    - task.progress (row count updates)
    """

    def __init__(self) -> None:
        # Subscribers for specific event types
        self._type_subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        # Subscribers for all events
        self._all_subscribers: set[asyncio.Queue] = set()
        # Track subscriber metadata for filtering
        self._subscriber_meta: dict[asyncio.Queue, dict[str, Any]] = {}

    def subscribe(
        self,
        event_types: list[str] | None = None,
        flow_id: str | None = None,
    ) -> asyncio.Queue:
        """
        Subscribe to events.

        Args:
            event_types: List of event types to subscribe to (None = all)
            flow_id: Filter events to specific flow (None = all flows)

        Returns:
            Queue that will receive events
        """
        queue: asyncio.Queue = asyncio.Queue()

        # Store metadata for filtering
        self._subscriber_meta[queue] = {
            "event_types": event_types,
            "flow_id": flow_id,
        }

        # Register for event types
        if event_types:
            for event_type in event_types:
                self._type_subscribers[event_type].add(queue)
        else:
            self._all_subscribers.add(queue)

        logger.debug(f"New subscriber: types={event_types}, flow_id={flow_id}")
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        """Remove subscriber from all event types."""
        # Remove from all-events subscribers
        self._all_subscribers.discard(queue)

        # Remove from type-specific subscribers
        for subscribers in self._type_subscribers.values():
            subscribers.discard(queue)

        # Clean up metadata
        self._subscriber_meta.pop(queue, None)

        logger.debug("Subscriber removed")

    async def emit(
        self,
        event_type: str,
        data: dict[str, Any],
        flow_id: str | None = None,
    ) -> int:
        """
        Emit an event to all relevant subscribers.

        Args:
            event_type: Type of event (e.g., "task.completed")
            data: Event payload
            flow_id: Associated flow ID for filtering

        Returns:
            Number of subscribers notified
        """
        event = {
            "event": event_type,
            "timestamp": datetime.now(UTC).isoformat(),
            "data": data,
        }

        notified = 0

        # Get relevant subscribers
        subscribers = set()
        subscribers.update(self._all_subscribers)
        subscribers.update(self._type_subscribers.get(event_type, set()))

        for queue in subscribers:
            # Check flow_id filter
            meta = self._subscriber_meta.get(queue, {})
            subscriber_flow_id = meta.get("flow_id")

            if subscriber_flow_id and flow_id and subscriber_flow_id != flow_id:
                continue

            try:
                await queue.put(event)
                notified += 1
            except Exception as e:
                logger.warning(f"Failed to send event to subscriber: {e}")

        if notified > 0:
            logger.debug(
                f"Emitted {event_type} to {notified} subscribers",
                extra={"event_type": event_type, "flow_id": flow_id},
            )

        return notified

    def subscriber_count(self) -> int:
        """Get total number of active subscribers."""
        all_queues = set(self._all_subscribers)
        for subscribers in self._type_subscribers.values():
            all_queues.update(subscribers)
        return len(all_queues)


def format_sse_event(event: dict[str, Any]) -> bytes:
    """
    Format event for Server-Sent Events protocol.

    Args:
        event: Event dict with "event" and "data" keys

    Returns:
        Bytes formatted for SSE stream
    """
    event_type = event.get("event", "message")
    data = json.dumps(event.get("data", {}))

    lines = [
        f"event: {event_type}",
        f"data: {data}",
        "",  # Empty line terminates event
    ]
    return "\n".join(lines).encode("utf-8") + b"\n"
