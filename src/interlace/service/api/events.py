"""
Event bus for real-time updates via Server-Sent Events (SSE).

Re-exports the core EventBus and adds SSE-specific formatting.
"""

import json
from typing import Any

# Re-export core EventBus so existing imports continue to work
from interlace.core.events import EventBus

__all__ = ["EventBus", "format_sse_event"]


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
