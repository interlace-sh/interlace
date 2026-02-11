"""
Stream API endpoints.

Provides HTTP endpoints for publishing to streams, listing streams,
subscribing via SSE, and managing stream consumers.
"""

import asyncio
import json
import time
import uuid
from typing import Any, Dict

from aiohttp import web

from interlace.service.api.errors import (
    APIError,
    ErrorCode,
    NotFoundError,
    ValidationError,
)
from interlace.service.api.handlers import BaseHandler
from interlace.service.api.events import format_sse_event
from interlace.utils.logging import get_logger

logger = get_logger("interlace.api.streams")


class StreamsHandler(BaseHandler):
    """Handler for stream publishing, listing, and subscription endpoints."""

    async def list(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/streams

        List all registered streams with metadata.

        Query params:
            schema: Filter by schema name
            tag: Filter by tag
        """
        schema_filter = request.query.get("schema")
        tag_filter = request.query.get("tag")

        streams = []
        for name, info in self.models.items():
            if info.get("type") != "stream":
                continue

            if schema_filter and info.get("schema") != schema_filter:
                continue

            if tag_filter and tag_filter not in (info.get("tags") or []):
                continue

            # Get downstream dependents
            dependents = []
            if self.graph:
                dependents = self.graph.get_dependents(name) or []

            streams.append({
                "name": name,
                "schema": info.get("schema", "events"),
                "description": info.get("description"),
                "cursor": info.get("cursor", "rowid"),
                "tags": info.get("tags", []),
                "owner": info.get("owner"),
                "fields": self._serialize_fields(info.get("fields")),
                "auth_required": bool(info.get("auth")),
                "validate_schema": info.get("validate_schema", False),
                "downstream_models": dependents,
                "endpoint": f"/api/v1/streams/{name}",
            })

        return await self.json_response(
            {"streams": streams, "count": len(streams)},
            request=request,
        )

    async def get(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/streams/{name}

        Get detailed stream information.
        """
        name = request.match_info["name"]
        info = self.models.get(name)

        if not info or info.get("type") != "stream":
            raise NotFoundError("Stream", name, ErrorCode.RESOURCE_NOT_FOUND)

        # Get downstream dependents
        dependents = []
        if self.graph:
            dependents = self.graph.get_dependents(name) or []

        # Try to get row count
        row_count = None
        try:
            from interlace.connections.manager import get_connection as get_named_connection
            conn_name = info.get("connection") or self._get_default_connection()
            if conn_name:
                wrapper = get_named_connection(conn_name)
                con = wrapper.connection
                schema = info.get("schema", "events")
                try:
                    tables = con.list_tables(database=schema)
                    if name in tables:
                        row_count = con.table(name, database=schema).count().execute()
                except Exception:
                    pass
        except Exception:
            pass

        stream_detail = {
            "name": name,
            "schema": info.get("schema", "events"),
            "description": info.get("description"),
            "cursor": info.get("cursor", "rowid"),
            "tags": info.get("tags", []),
            "owner": info.get("owner"),
            "fields": self._serialize_fields(info.get("fields")),
            "auth_required": bool(info.get("auth")),
            "validate_schema": info.get("validate_schema", False),
            "rate_limit": info.get("rate_limit"),
            "retention": info.get("retention"),
            "downstream_models": dependents,
            "row_count": row_count,
            "endpoints": {
                "publish": f"/api/v1/streams/{name}",
                "subscribe": f"/api/v1/streams/{name}/subscribe",
                "consume": f"/api/v1/streams/{name}/consume",
            },
        }

        return await self.json_response(stream_detail, request=request)

    async def publish(self, request: web.Request) -> web.Response:
        """
        POST /api/v1/streams/{name}

        Publish event(s) to a stream.

        Request body:
            Single event: {"key": "value", ...}
            Batch: [{"key": "value"}, {"key": "value"}, ...]

        Headers:
            Content-Type: application/json
            Authorization: Bearer <token> (if auth configured)

        Returns 202 Accepted.
        """
        name = request.match_info["name"]
        info = self.models.get(name)

        if not info or info.get("type") != "stream":
            raise NotFoundError("Stream", name, ErrorCode.RESOURCE_NOT_FOUND)

        # Check authentication
        auth_config = info.get("auth")
        if auth_config:
            self._check_auth(request, auth_config)

        # Check rate limit
        rate_limit = info.get("rate_limit")
        if rate_limit:
            self._check_rate_limit(request, name, rate_limit)

        # Parse body
        try:
            payload = await request.json()
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON: {e}")

        if isinstance(payload, dict):
            rows = [payload]
        elif isinstance(payload, list) and all(isinstance(x, dict) for x in payload):
            rows = payload
        else:
            raise ValidationError("Body must be a JSON object or array of objects")

        if not rows:
            return await self.json_response(
                {
                    "status": "accepted",
                    "stream": name,
                    "rows_received": 0,
                },
                status=202,
                request=request,
            )

        # Use publish() for actual ingestion
        from interlace.core.stream import publish

        # Resolve connection
        conn_name = info.get("connection") or self._get_default_connection()
        con = None
        if conn_name:
            try:
                from interlace.connections.manager import get_connection as get_named_connection
                wrapper = get_named_connection(conn_name)
                con = wrapper.connection
            except Exception:
                pass

        result = await publish(
            name,
            rows,
            _connection_obj=con,
            _config=self.config,
            _event_bus=self.event_bus,
            _graph=self.graph,
            _enqueue_run=self.service._enqueue_run if hasattr(self.service, "_enqueue_run") else None,
        )

        return await self.json_response(result, status=202, request=request)

    async def subscribe_sse(self, request: web.Request) -> web.StreamResponse:
        """
        GET /api/v1/streams/{name}/subscribe

        Subscribe to real-time stream events via Server-Sent Events (SSE).

        Query params:
            filter: JSON filter expression (optional)

        Returns:
            SSE stream of events as they are published.
        """
        name = request.match_info["name"]
        info = self.models.get(name)

        if not info or info.get("type") != "stream":
            raise NotFoundError("Stream", name, ErrorCode.RESOURCE_NOT_FOUND)

        # Setup SSE response
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)

        # Subscribe to stream events via in-process listener
        from interlace.core.stream import _stream_listeners

        queue: asyncio.Queue = asyncio.Queue()
        if name not in _stream_listeners:
            _stream_listeners[name] = []
        _stream_listeners[name].append(queue)

        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)

                    sse_event = format_sse_event({
                        "event": "stream.event",
                        "data": {
                            "stream": name,
                            "event": event,
                        },
                    })
                    await response.write(sse_event)

                except asyncio.TimeoutError:
                    # Keepalive
                    await response.write(b": keepalive\n\n")
                except ConnectionResetError:
                    break

        except asyncio.CancelledError:
            pass
        finally:
            # Cleanup
            if name in _stream_listeners:
                try:
                    _stream_listeners[name].remove(queue)
                except ValueError:
                    pass

        return response

    async def consume_batch(self, request: web.Request) -> web.Response:
        """
        POST /api/v1/streams/{name}/consume

        Consume a batch of unprocessed events (cursor-based).

        Request body:
            consumer: Consumer name/ID (required)
            batch_size: Max events to return (default 100)

        Returns:
            {"events": [...], "count": N, "cursor": "..."}
        """
        name = request.match_info["name"]
        info = self.models.get(name)

        if not info or info.get("type") != "stream":
            raise NotFoundError("Stream", name, ErrorCode.RESOURCE_NOT_FOUND)

        try:
            body = await request.json()
        except Exception:
            body = {}

        consumer_name = body.get("consumer")
        if not consumer_name:
            raise ValidationError("'consumer' is required")

        batch_size = int(body.get("batch_size", 100))
        if batch_size < 1 or batch_size > 10000:
            raise ValidationError("'batch_size' must be between 1 and 10000")

        from interlace.core.stream import consume

        # Resolve connection
        conn_name = info.get("connection") or self._get_default_connection()
        con = None
        if conn_name:
            try:
                from interlace.connections.manager import get_connection as get_named_connection
                wrapper = get_named_connection(conn_name)
                con = wrapper.connection
            except Exception:
                pass

        events = await consume(
            name,
            consumer_name,
            batch_size=batch_size,
            _connection_obj=con,
            _config=self.config,
        )

        return await self.json_response(
            {
                "stream": name,
                "consumer": consumer_name,
                "events": events,
                "count": len(events),
            },
            request=request,
        )

    async def ack_events(self, request: web.Request) -> web.Response:
        """
        POST /api/v1/streams/{name}/ack

        Acknowledge processed events and advance consumer cursor.

        Request body:
            consumer: Consumer name/ID (required)
            events: List of events with _interlace_rowid (required)
        """
        name = request.match_info["name"]
        info = self.models.get(name)

        if not info or info.get("type") != "stream":
            raise NotFoundError("Stream", name, ErrorCode.RESOURCE_NOT_FOUND)

        try:
            body = await request.json()
        except Exception:
            raise ValidationError("Request body must be valid JSON")

        consumer_name = body.get("consumer")
        if not consumer_name:
            raise ValidationError("'consumer' is required")

        events = body.get("events", [])
        if not events:
            return await self.json_response(
                {"status": "ok", "acknowledged": 0},
                request=request,
            )

        from interlace.core.stream import ack

        conn_name = info.get("connection") or self._get_default_connection()
        con = None
        if conn_name:
            try:
                from interlace.connections.manager import get_connection as get_named_connection
                wrapper = get_named_connection(conn_name)
                con = wrapper.connection
            except Exception:
                pass

        await ack(
            name,
            consumer_name,
            events,
            _connection_obj=con,
            _config=self.config,
        )

        return await self.json_response(
            {"status": "ok", "acknowledged": len(events)},
            request=request,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_default_connection(self) -> str | None:
        """Get the default connection name from config."""
        return (
            self.config.get("executor", {}).get("default_connection")
            or next(iter(self.config.get("connections", {}).keys()), None)
        )

    def _serialize_fields(self, fields: Any) -> Any:
        """Serialize fields to a JSON-safe representation."""
        if fields is None:
            return None
        if isinstance(fields, dict):
            return {k: str(v) for k, v in fields.items()}
        if isinstance(fields, list):
            return [{"name": f[0], "type": str(f[1])} for f in fields if len(f) >= 2]
        # ibis.Schema
        if hasattr(fields, "names"):
            return {
                name: str(fields[name])
                for name in fields.names
            }
        return str(fields)

    def _check_auth(self, request: web.Request, auth_config: Dict[str, Any]) -> None:
        """Validate request authentication against stream auth config."""
        auth_type = auth_config.get("type", "bearer")

        if auth_type == "bearer":
            expected_token = auth_config.get("token")
            if not expected_token:
                return  # No token configured, skip auth

            auth_header = request.headers.get("Authorization", "")
            if not auth_header.startswith("Bearer "):
                raise APIError(
                    code=ErrorCode.UNAUTHORIZED,
                    message="Missing or invalid Authorization header",
                    status=401,
                )
            token = auth_header[7:]
            if token != expected_token:
                raise APIError(
                    code=ErrorCode.UNAUTHORIZED,
                    message="Invalid bearer token",
                    status=401,
                )

        elif auth_type == "api_key":
            header_name = auth_config.get("header", "X-API-Key")
            expected_key = auth_config.get("key")
            if not expected_key:
                return

            provided_key = request.headers.get(header_name, "")
            if provided_key != expected_key:
                raise APIError(
                    code=ErrorCode.UNAUTHORIZED,
                    message=f"Invalid API key in {header_name}",
                    status=401,
                )

    def _check_rate_limit(
        self,
        request: web.Request,
        stream_name: str,
        rate_config: Dict[str, Any],
    ) -> None:
        """
        Basic rate limiting check.

        For production, this should use a proper rate limiter (e.g., Redis-backed).
        This implementation uses a simple in-memory token bucket.
        """
        # Rate limiting is best-effort in this implementation
        # A production system would use Redis or similar
        max_rps = rate_config.get("requests_per_second", 0)
        if max_rps <= 0:
            return

        # Simple check using service-level state
        rate_key = f"_rate_{stream_name}"
        now = time.time()

        if not hasattr(self.service, "_rate_state"):
            self.service._rate_state = {}

        state = self.service._rate_state.get(rate_key, {"count": 0, "window_start": now})

        # Reset window every second
        if now - state["window_start"] >= 1.0:
            state = {"count": 0, "window_start": now}

        state["count"] += 1
        self.service._rate_state[rate_key] = state

        if state["count"] > max_rps:
            raise APIError(
                code=ErrorCode.RATE_LIMITED,
                message=f"Rate limit exceeded for stream '{stream_name}' "
                        f"({max_rps} requests/second)",
                status=429,
                details={"limit": max_rps, "retry_after": 1},
            )
