"""
Server-Sent Events (SSE) endpoint for real-time updates.
"""

import asyncio

from aiohttp import web

from interlace.service.api.events import format_sse_event
from interlace.service.api.handlers import BaseHandler
from interlace.utils.logging import get_logger

logger = get_logger("interlace.api.handlers.events")


class EventsHandler(BaseHandler):
    """Handler for SSE real-time event streaming."""

    async def stream(self, request: web.Request) -> web.StreamResponse:
        """
        GET /api/v1/events

        Server-Sent Events endpoint for real-time updates.

        Query params:
            flow_id: Subscribe to events for specific flow only
            types: Comma-separated event types to subscribe to

        Event types:
            - flow.started, flow.completed, flow.failed, flow.cancelled
            - task.enqueued, task.waiting, task.ready, task.running
            - task.materialising, task.completed, task.failed, task.skipped
            - task.progress
        """
        # Parse query params
        flow_id = request.query.get("flow_id")
        types_param = request.query.get("types")
        event_types: list[str] | None = None
        if types_param:
            event_types = [t.strip() for t in types_param.split(",")]

        # Setup SSE response
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering
            },
        )

        # Add request ID if available
        request_id = self.get_request_id(request)
        if request_id:
            response.headers["X-Request-ID"] = request_id

        await response.prepare(request)

        # Subscribe to events
        if not self.event_bus:
            # No event bus available, send error and close
            error_event = {
                "event": "error",
                "data": {"message": "Event bus not available"},
            }
            await response.write(format_sse_event(error_event))
            return response

        queue = self.event_bus.subscribe(event_types=event_types, flow_id=flow_id)

        logger.info(
            "SSE client connected",
            extra={
                "flow_id": flow_id,
                "event_types": event_types,
                "request_id": request_id,
            },
        )

        try:
            # Send initial connection event
            await response.write(
                format_sse_event(
                    {
                        "event": "connected",
                        "data": {
                            "message": "Connected to event stream",
                            "subscribed_types": event_types,
                            "flow_id": flow_id,
                        },
                    }
                )
            )

            # Stream events
            while True:
                try:
                    # Wait for event with timeout for keepalive
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    if event is None:
                        # Shutdown sentinel received
                        break
                    try:
                        await response.write(format_sse_event(event))
                    except (ConnectionResetError, BrokenPipeError, OSError) as e:
                        logger.debug(f"SSE write failed, client disconnected: {e}")
                        break
                except TimeoutError:
                    # Send keepalive comment
                    try:
                        await response.write(b": keepalive\n\n")
                    except (ConnectionResetError, BrokenPipeError, OSError) as e:
                        logger.debug(f"SSE keepalive failed, client disconnected: {e}")
                        break
                except asyncio.CancelledError:
                    break

        except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            logger.warning(f"SSE stream error: {e}")
        finally:
            self.event_bus.unsubscribe(queue)
            logger.info("SSE client disconnected")

        return response
