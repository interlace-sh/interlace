"""
Stream decorator and publish/subscribe API for event ingestion.

Streams are durable append-only tables that serve as the ingestion layer.
They provide:
- Automatic HTTP endpoints for webhooks (/streams/{name})
- Programmatic publish() API for event ingestion
- Async subscribe() for consuming stream events
- Message queue adapter pattern for Kafka/RabbitMQ/Redis/etc.
"""

from __future__ import annotations

import asyncio
import functools
import uuid
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import (
    Any,
)

import ibis

from interlace.utils.logging import get_logger

logger = get_logger("interlace.stream")


# ---------------------------------------------------------------------------
# Stream registry (in-process)
# ---------------------------------------------------------------------------

_stream_registry: dict[str, dict[str, Any]] = {}
try:
    _stream_lock = asyncio.Lock() if asyncio.get_event_loop().is_running() else None
except RuntimeError:
    _stream_lock = None

# Listeners for subscribe() -- maps stream_name -> list[asyncio.Queue]
_stream_listeners: dict[str, list[asyncio.Queue]] = {}


def _get_stream_info(name: str) -> dict[str, Any] | None:
    """Get stream metadata from registry."""
    return _stream_registry.get(name)


def _register_stream(name: str, info: dict[str, Any]) -> None:
    """Register a stream in the in-process registry."""
    _stream_registry[name] = info


# ---------------------------------------------------------------------------
# @stream decorator
# ---------------------------------------------------------------------------


def stream(
    name: str | None = None,
    schema: str = "events",
    connection: str | None = None,
    cursor: str = "rowid",
    fields: dict[str, Any] | list[tuple[str, Any]] | ibis.Schema | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    owner: str | None = None,
    # Webhook/API config
    auth: dict[str, Any] | None = None,
    rate_limit: dict[str, Any] | None = None,
    validate_schema: bool = False,
    # Retention
    retention: dict[str, Any] | None = None,
    **kwargs,
):
    """
    Decorator to define an Interlace stream (append-only ingestion table).

    Streams are not executed like normal models. They serve as named
    ingestion tables that can receive data via:
    - publish() API (programmatic)
    - HTTP endpoints (/streams/{name}) when serve() is running
    - Message queue adapters (Kafka, RabbitMQ, etc.)

    Args:
        name: Stream name (defaults to function name)
        schema: Database schema (default: "events")
        connection: Named connection to use (default: project default)
        cursor: Cursor column for incremental processing ("rowid" or datetime column)
        fields: Schema definition for the stream table
        description: Human-readable description
        tags: Tags for categorization
        owner: Owner identifier
        auth: Authentication config for HTTP endpoint
            {"type": "bearer", "token": "..."} or {"type": "api_key", "header": "X-API-Key"}
        rate_limit: Rate limiting config
            {"requests_per_second": 100, "burst": 200}
        validate_schema: Whether to validate incoming data against fields schema
        retention: Data retention config
            {"max_age_days": 30} or {"max_rows": 1_000_000}
    """

    def decorator(func: Callable) -> Callable:
        stream_name = name or func.__name__

        stream_meta = {
            "name": stream_name,
            "schema": schema,
            "connection": connection,
            "cursor": cursor,
            "fields": fields,
            "description": description or func.__doc__,
            "tags": tags or [],
            "owner": owner,
            "auth": auth,
            "rate_limit": rate_limit,
            "validate_schema": validate_schema,
            "retention": retention,
            **kwargs,
        }

        func._interlace_stream = stream_meta

        # Expose as model-like for discovery/graph integration.
        func._interlace_model = {
            "name": stream_name,
            "schema": schema,
            "connection": connection,
            "materialise": "table",
            "strategy": "append",
            "primary_key": None,
            "dependencies": [],
            "incremental": None,
            "description": description or func.__doc__,
            "tags": tags or [],
            "owner": owner,
            "fields": fields,
            "type": "stream",
            "auth": auth,
            "rate_limit": rate_limit,
            "validate_schema": validate_schema,
            "retention": retention,
            **kwargs,
        }

        # Register in global stream registry
        _register_stream(stream_name, func._interlace_model)

        @functools.wraps(func)
        def wrapper(*args, **kw):
            # Stream functions are not meant to run directly
            return None

        # Preserve metadata on wrapper
        wrapper._interlace_stream = func._interlace_stream
        wrapper._interlace_model = func._interlace_model
        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# publish() -- core programmatic API
# ---------------------------------------------------------------------------


async def publish(
    stream: str | Callable,
    data: dict[str, Any] | list[dict[str, Any]],
    *,
    connection: str | None = None,
    metadata: dict[str, Any] | None = None,
    _connection_obj: ibis.BaseBackend | None = None,
    _config: dict[str, Any] | None = None,
    _event_bus: Any | None = None,
    _graph: Any | None = None,
    _enqueue_run: Callable | None = None,
) -> dict[str, Any]:
    """
    Publish event(s) to a stream.

    Can be used standalone or integrated with the Interlace service.
    Events are appended to the stream table and downstream models
    are triggered automatically when running inside the service.

    Args:
        stream: Stream name (str) or decorated stream function reference
        data: Single event dict or list of event dicts
        connection: Override connection name
        metadata: Optional metadata to attach (not stored in table)
        _connection_obj: Internal: pre-resolved ibis connection
        _config: Internal: project config dict
        _event_bus: Internal: EventBus instance for SSE notifications
        _graph: Internal: dependency graph for downstream triggering
        _enqueue_run: Internal: callback to trigger downstream models

    Returns:
        Dict with publish result:
        {
            "status": "accepted",
            "stream": "...",
            "rows_received": N,
            "publish_id": "...",
            "triggered_models": [...]
        }

    Example:
        # Single event
        await publish("user_events", {"user_id": "123", "action": "signup"})

        # Batch
        await publish("user_events", [
            {"user_id": "123", "action": "signup"},
            {"user_id": "124", "action": "login"},
        ])

        # From decorated stream reference
        @stream(name="orders")
        def orders(): pass

        await publish(orders, {"order_id": "456", "total": 99.99})
    """
    # Resolve stream name
    if callable(stream):
        stream_meta = getattr(stream, "_interlace_stream", None)
        if stream_meta:
            stream_name = stream_meta["name"]
        else:
            stream_name = getattr(stream, "__name__", str(stream))
    else:
        stream_name = stream

    # Normalize data to list
    if isinstance(data, dict):
        rows = [data]
    elif isinstance(data, list) and all(isinstance(x, dict) for x in data):
        rows = data
    else:
        raise ValueError("data must be a dict or list of dicts")

    if not rows:
        return {
            "status": "accepted",
            "stream": stream_name,
            "rows_received": 0,
            "publish_id": str(uuid.uuid4()),
            "triggered_models": [],
        }

    publish_id = str(uuid.uuid4())

    # Add interlace metadata columns if not present
    now = datetime.now(UTC).isoformat()
    for row in rows:
        row.setdefault("_interlace_published_at", now)
        row.setdefault("_interlace_publish_id", publish_id)

    # Get stream info
    stream_info = _get_stream_info(stream_name)
    db_schema = "events"
    conn_name = connection

    if stream_info:
        db_schema = stream_info.get("schema", "events")
        if not conn_name:
            conn_name = stream_info.get("connection")

    # Resolve connection
    con = _connection_obj
    if con is None:
        con = _resolve_connection(conn_name, _config)

    if con is None:
        raise RuntimeError(
            f"Cannot publish to stream '{stream_name}': no connection available. "
            f"Either pass connection= or ensure interlace is initialized."
        )

    # Ensure schema exists
    _ensure_schema(con, db_schema)

    # Ensure table exists
    _ensure_stream_table(con, stream_name, db_schema, stream_info)

    # Validate schema if configured
    if stream_info and stream_info.get("validate_schema") and stream_info.get("fields"):
        _validate_event_schema(rows, stream_info["fields"], stream_name)

    # Insert rows
    _insert_rows(con, stream_name, db_schema, rows)

    # Notify listeners (in-process subscribers)
    await _notify_listeners(stream_name, rows, publish_id)

    # Emit to event bus (for SSE clients)
    if _event_bus:
        await _event_bus.emit(
            "stream.published",
            {
                "stream": stream_name,
                "rows_received": len(rows),
                "publish_id": publish_id,
            },
        )

    # Trigger downstream models
    triggered = []
    if _graph and _enqueue_run:
        dependents = _graph.get_dependents(stream_name) or []
        if dependents:
            triggered = dependents
            await _enqueue_run(dependents, force=True)

    logger.info(
        f"Published {len(rows)} event(s) to stream '{stream_name}' "
        f"(publish_id={publish_id}, triggered={len(triggered)} models)"
    )

    return {
        "status": "accepted",
        "stream": stream_name,
        "rows_received": len(rows),
        "publish_id": publish_id,
        "triggered_models": triggered,
    }


def publish_sync(
    stream: str | Callable,
    data: dict[str, Any] | list[dict[str, Any]],
    **kwargs,
) -> dict[str, Any]:
    """
    Synchronous wrapper for publish().

    Usage:
        from interlace import publish_sync

        publish_sync("user_events", {"user_id": "123", "action": "signup"})
    """
    try:
        asyncio.get_running_loop()
        # Already in an async context -- schedule as a task
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(asyncio.run, publish(stream, data, **kwargs))
            return future.result()
    except RuntimeError:
        # No running loop -- create one
        return asyncio.run(publish(stream, data, **kwargs))


# ---------------------------------------------------------------------------
# subscribe() -- async event consumer
# ---------------------------------------------------------------------------


async def subscribe(
    stream_name: str,
    *,
    batch_size: int = 1,
    timeout: float | None = None,
    filter_fn: Callable[[dict[str, Any]], bool] | None = None,
) -> AsyncIterator[dict[str, Any] | list[dict[str, Any]]]:
    """
    Subscribe to real-time events from a stream.

    Returns an async iterator that yields events as they are published.
    Events are received in-process (no network overhead when in same process).

    Args:
        stream_name: Name of the stream to subscribe to
        batch_size: Number of events to batch before yielding (1 = one at a time)
        timeout: Optional timeout in seconds between events (None = wait forever)
        filter_fn: Optional filter function - only yield events where filter_fn(event) is True

    Yields:
        Single event dict (batch_size=1) or list of event dicts (batch_size > 1)

    Example:
        async for event in subscribe("user_events"):
            print(f"New user event: {event}")

        # With batching
        async for batch in subscribe("user_events", batch_size=10, timeout=5.0):
            process_batch(batch)

        # With filtering
        async for event in subscribe("user_events", filter_fn=lambda e: e.get("type") == "signup"):
            handle_signup(event)
    """
    queue: asyncio.Queue = asyncio.Queue()

    # Register listener
    if stream_name not in _stream_listeners:
        _stream_listeners[stream_name] = []
    _stream_listeners[stream_name].append(queue)

    try:
        batch: list[dict[str, Any]] = []

        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=timeout)

                # Apply filter
                if filter_fn and not filter_fn(event):
                    continue

                if batch_size <= 1:
                    yield event
                else:
                    batch.append(event)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []

            except TimeoutError:
                # On timeout, yield partial batch if any
                if batch:
                    yield batch
                    batch = []
                elif timeout is not None:
                    # Yield empty to signal timeout (caller can decide to break)
                    continue

    finally:
        # Cleanup listener
        if stream_name in _stream_listeners:
            try:
                _stream_listeners[stream_name].remove(queue)
            except ValueError:
                pass
            if not _stream_listeners[stream_name]:
                del _stream_listeners[stream_name]


# ---------------------------------------------------------------------------
# Stream cursor consumer (database-backed consumption)
# ---------------------------------------------------------------------------


async def consume(
    stream_name: str,
    consumer_name: str,
    *,
    batch_size: int = 100,
    connection: str | None = None,
    _connection_obj: ibis.BaseBackend | None = None,
    _config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Consume unprocessed events from a stream using cursor-based tracking.

    Unlike subscribe() which is real-time in-process, consume() reads from
    the database and tracks position via cursors. Use this for:
    - Batch processing of accumulated events
    - Reliable at-least-once processing
    - Multi-process/distributed consumers

    Args:
        stream_name: Name of the stream to consume from
        consumer_name: Unique consumer identifier (tracks cursor position)
        batch_size: Maximum number of events to return
        connection: Override connection name
        _connection_obj: Internal: pre-resolved connection
        _config: Internal: project config

    Returns:
        List of event dicts (empty if no new events)

    Example:
        events = await consume("user_events", "analytics_worker", batch_size=100)
        for event in events:
            process(event)
        await ack("user_events", "analytics_worker", events)
    """
    stream_info = _get_stream_info(stream_name)
    db_schema = "events"
    if stream_info:
        db_schema = stream_info.get("schema", "events")

    con = _connection_obj
    if con is None:
        con = _resolve_connection(connection or (stream_info or {}).get("connection"), _config)

    if con is None:
        raise RuntimeError(f"Cannot consume from stream '{stream_name}': no connection available.")

    # Get cursor position for this consumer
    cursor_pos = _get_consumer_cursor(con, stream_name, consumer_name)

    # Read events after cursor
    try:
        from interlace.core.context import _execute_sql_internal

        # Use rowid-based cursor (default)
        if cursor_pos is not None:
            sql = (
                f"SELECT *, rowid AS _interlace_rowid FROM {db_schema}.{stream_name} "
                f"WHERE rowid > {cursor_pos} "
                f"ORDER BY rowid ASC LIMIT {batch_size}"
            )
        else:
            sql = (
                f"SELECT *, rowid AS _interlace_rowid FROM {db_schema}.{stream_name} "
                f"ORDER BY rowid ASC LIMIT {batch_size}"
            )

        result = _execute_sql_internal(con, sql)
        if result is not None and hasattr(result, "to_dict"):
            return result.to_dict("records")
        elif result is not None and hasattr(result, "fetchdf"):
            df = result.fetchdf()
            return df.to_dict("records")
        return []
    except Exception as e:
        logger.warning(f"Failed to consume from stream '{stream_name}': {e}")
        return []


async def ack(
    stream_name: str,
    consumer_name: str,
    events: list[dict[str, Any]],
    *,
    connection: str | None = None,
    _connection_obj: ibis.BaseBackend | None = None,
    _config: dict[str, Any] | None = None,
) -> None:
    """
    Acknowledge processed events and advance consumer cursor.

    Args:
        stream_name: Stream name
        consumer_name: Consumer identifier
        events: List of processed events (must contain _interlace_rowid)
        connection: Override connection name
    """
    if not events:
        return

    # Find max rowid
    max_rowid = max(e.get("_interlace_rowid", 0) for e in events)
    if max_rowid <= 0:
        return

    stream_info = _get_stream_info(stream_name)
    con = _connection_obj
    if con is None:
        con = _resolve_connection(connection or (stream_info or {}).get("connection"), _config)

    if con is None:
        raise RuntimeError(f"Cannot ack stream '{stream_name}': no connection available.")

    _update_consumer_cursor(con, stream_name, consumer_name, max_rowid)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_connection(conn_name: str | None, config: dict[str, Any] | None) -> ibis.BaseBackend | None:
    """Resolve an ibis connection from name or config."""
    try:
        from interlace.connections.manager import get_connection as get_named_connection

        if conn_name:
            wrapper = get_named_connection(conn_name)
            return wrapper.connection

        # Try default connection
        if config:
            default_conn = config.get("executor", {}).get("default_connection") or next(
                iter(config.get("connections", {}).keys()), None
            )
            if default_conn:
                wrapper = get_named_connection(default_conn)
                return wrapper.connection
    except Exception as e:
        logger.debug(f"Failed to resolve connection: {e}")
    return None


def _ensure_schema(con: ibis.BaseBackend, schema: str) -> None:
    """Ensure database schema exists."""
    try:
        if hasattr(con, "create_database"):
            con.create_database(schema, force=False)
    except Exception:
        pass


def _ensure_stream_table(
    con: ibis.BaseBackend,
    stream_name: str,
    schema: str,
    stream_info: dict[str, Any] | None,
) -> None:
    """Ensure stream table exists, creating it from fields schema if needed."""
    try:
        existing = con.list_tables(database=schema)
        if stream_name in existing:
            return
    except Exception:
        pass

    # Try creating from declared schema
    if stream_info and stream_info.get("fields"):
        try:
            from interlace.utils.schema_utils import fields_to_ibis_schema

            ibis_schema = fields_to_ibis_schema(stream_info["fields"])
            if ibis_schema:
                con.create_table(stream_name, schema=ibis_schema, database=schema, overwrite=False)
                return
        except Exception:
            pass

    # Create minimal placeholder (will be extended on first insert)
    try:
        con.create_table(
            stream_name,
            schema=ibis.schema(
                {
                    "_interlace_published_at": "string",
                    "_interlace_publish_id": "string",
                }
            ),
            database=schema,
            overwrite=False,
        )
    except Exception:
        pass


def _validate_event_schema(
    rows: list[dict[str, Any]],
    fields: Any,
    stream_name: str,
) -> None:
    """Validate incoming events against declared fields schema."""
    try:
        from interlace.utils.schema_utils import fields_to_ibis_schema

        ibis_schema = fields_to_ibis_schema(fields)
        if not ibis_schema:
            return

        required_cols = set(ibis_schema.names)
        # Don't require interlace metadata columns
        required_cols -= {"_interlace_published_at", "_interlace_publish_id"}

        for i, row in enumerate(rows):
            missing = required_cols - set(row.keys())
            if missing:
                raise ValueError(f"Event {i} for stream '{stream_name}' missing required fields: {missing}")
    except ImportError:
        pass


def _insert_rows(
    con: ibis.BaseBackend,
    stream_name: str,
    schema: str,
    rows: list[dict[str, Any]],
) -> None:
    """Insert rows into stream table via temp table."""
    from interlace.core.context import _execute_sql_internal

    tmp_name = f"_interlace_stream_{stream_name}_{uuid.uuid4().hex[:12]}"
    tmp_expr = ibis.memtable(rows)

    try:
        # Create temp table
        try:
            con.create_table(tmp_name, obj=tmp_expr, temp=True)
        except Exception:
            df = tmp_expr.execute()
            con.create_table(tmp_name, obj=df, temp=True)

        # Insert into stream table
        _execute_sql_internal(con, f"INSERT INTO {schema}.{stream_name} SELECT * FROM {tmp_name}")
    finally:
        try:
            if hasattr(con, "drop_table"):
                con.drop_table(tmp_name, force=True)
            else:
                _execute_sql_internal(con, f"DROP TABLE IF EXISTS {tmp_name}")
        except Exception:
            pass


async def _notify_listeners(
    stream_name: str,
    rows: list[dict[str, Any]],
    publish_id: str,
) -> None:
    """Notify in-process subscribers of new events."""
    listeners = _stream_listeners.get(stream_name, [])
    if not listeners:
        return

    dead_listeners = []
    for queue in listeners:
        try:
            for row in rows:
                await queue.put(row)
        except Exception:
            dead_listeners.append(queue)

    # Cleanup dead listeners
    for q in dead_listeners:
        try:
            _stream_listeners[stream_name].remove(q)
        except (ValueError, KeyError):
            pass


def _get_consumer_cursor(
    con: ibis.BaseBackend,
    stream_name: str,
    consumer_name: str,
) -> int | None:
    """Get the current cursor position for a consumer."""
    from interlace.core.context import _execute_sql_internal

    try:
        result = _execute_sql_internal(
            con,
            f"SELECT last_cursor FROM interlace.stream_consumers "
            f"WHERE stream_name = '{stream_name}' AND consumer_name = '{consumer_name}'",
        )
        if result is not None:
            if hasattr(result, "fetchone"):
                row = result.fetchone()
                if row:
                    return int(row[0])
            elif hasattr(result, "to_dict"):
                records = result.to_dict("records")
                if records:
                    return int(records[0]["last_cursor"])
    except Exception:
        pass
    return None


def _update_consumer_cursor(
    con: ibis.BaseBackend,
    stream_name: str,
    consumer_name: str,
    cursor_value: int,
) -> None:
    """Update cursor position for a consumer."""
    from interlace.core.context import _execute_sql_internal

    try:
        # Ensure table exists
        _execute_sql_internal(
            con,
            """
            CREATE TABLE IF NOT EXISTS interlace.stream_consumers (
                stream_name VARCHAR NOT NULL,
                consumer_name VARCHAR NOT NULL,
                last_cursor BIGINT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (stream_name, consumer_name)
            )
            """,
        )

        # Upsert cursor
        _execute_sql_internal(
            con,
            f"DELETE FROM interlace.stream_consumers "
            f"WHERE stream_name = '{stream_name}' AND consumer_name = '{consumer_name}'",
        )
        _execute_sql_internal(
            con,
            f"INSERT INTO interlace.stream_consumers "
            f"(stream_name, consumer_name, last_cursor, updated_at) "
            f"VALUES ('{stream_name}', '{consumer_name}', {cursor_value}, CURRENT_TIMESTAMP)",
        )
    except Exception as e:
        logger.warning(f"Failed to update consumer cursor: {e}")
