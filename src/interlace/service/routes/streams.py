"""Stream publish, tail, and commit."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from uuid import uuid4

from litestar import Request, get, post
from litestar.datastructures import State
from litestar.exceptions import ClientException
from litestar.params import FromPath, FromQuery
from litestar.response import ServerSentEvent, ServerSentEventMessage

from interlace.exceptions import StreamError
from interlace.service.present import (
    _log_name_or_404,
    _stream_or_404,
)
from interlace.service.types import (
    PublishResult,
    StreamCommit,
    StreamCommitResult,
    StreamDetail,
    StreamInfo,
)
from interlace.streaming.log import Event, Lease
from interlace.streaming.materializer import (
    quarantine_stream,
    stream_watermark,
)
from interlace.streaming.schema import (
    partition_rows,
    validate_rows,
    validate_rows_evolve,
)  # ---- live reload ---------------------------------------------------------------


@get("/streams")
async def get_streams(state: State) -> list[StreamInfo]:
    out = []
    for stream in state.streams.values():
        head = await state.stream_log.head(stream.name)
        watermark = await stream_watermark(stream, state.engine)
        out.append(
            StreamInfo(
                name=stream.name,
                schema=stream.schema,
                table=f"streams.{stream.name}",
                head=head,
                watermark=watermark,
                pending=max(0, head - watermark),
                on_schema_drift=stream.on_schema_drift,
                retention=stream.retention,
            )
        )
    return out


@get("/streams/{name:str}")
async def get_stream(name: FromPath[str], state: State) -> StreamDetail:
    stream = _stream_or_404(state, name)
    head = await state.stream_log.head(name)
    watermark = await stream_watermark(stream, state.engine)
    events = await state.stream_log.read(name, max(0, head - 20), 20)
    return StreamDetail(
        name=stream.name,
        schema=stream.schema,
        table=f"streams.{stream.name}",
        head=head,
        watermark=watermark,
        pending=max(0, head - watermark),
        idempotency_key=stream.idempotency_key,
        recent=[dict(event.payload, _offset=event.offset) for event in events],
        on_schema_drift=stream.on_schema_drift,
        retention=stream.retention,
    )


@post("/streams/{name:str}", opt={"scope": "write"})
async def publish(name: FromPath[str], data: dict | list, state: State) -> PublishResult:  # noqa: C901
    """Publish one event (object) or a batch (array). Durable before this returns.

    Backpressure: when the warehouse can't keep up, the durable-but-unmaterialized
    backlog grows without bound — past ``stream_max_pending`` this returns 429 so
    producers slow down instead of the daemon eating memory and disk. The counters
    are maintained by the publish/flush paths themselves; nothing here queries the
    warehouse.
    """
    stream = _stream_or_404(state, name)
    shadow_name = f"{name}__quarantine"
    pending = max(
        state.log_heads.get(name, 0) - state.flushed_heads.get(name, 0),
        state.log_heads.get(shadow_name, 0) - state.flushed_heads.get(shadow_name, 0),
    )
    if pending > state.stream_max_pending:
        from litestar.exceptions import HTTPException

        raise HTTPException(
            status_code=429,
            detail=f"stream {name!r} has {pending} unmaterialized events (limit {state.stream_max_pending}) — "
            f"the warehouse is behind; retry with backoff",
        )
    rows = data if isinstance(data, list) else [data]
    quarantined: list[tuple[object, str]] = []
    try:
        if stream.on_schema_drift == "evolve":
            validate_rows_evolve(stream, rows)  # unknown fields welcome; incompatible types still reject
        elif stream.on_schema_drift == "quarantine":
            rows, quarantined = partition_rows(stream, rows)
        else:
            validate_rows(stream, rows)
    except StreamError as exc:
        raise ClientException(detail=exc.message) from exc

    def _event(row: dict) -> Event:
        key = str(row[stream.idempotency_key]) if stream.idempotency_key and stream.idempotency_key in row else None
        return Event(payload=row, idempotency_key=key)

    result = await state.stream_log.append(name, [_event(row) for row in rows]) if rows else None
    if quarantined:  # failures are durable too: the shadow stream keeps error + raw payload
        shadow = quarantine_stream(stream)
        shadow_result = await state.stream_log.append(
            shadow.name, [Event(payload={"error": error, "payload": json.dumps(row)}) for row, error in quarantined]
        )
        if shadow_result.offsets:  # the shadow log is backpressured like any other
            state.log_heads[shadow.name] = max(state.log_heads.get(shadow.name, 0), max(shadow_result.offsets))
    if result and result.offsets:
        state.log_heads[name] = max(state.log_heads.get(name, 0), max(result.offsets))
    if result or quarantined:  # durable: hand materialization to the flusher micro-batch
        state.flush_dirty.add(name)
        if quarantined:
            state.flush_dirty.add(quarantine_stream(stream).name)
        state.flush_wanted.set()
    return PublishResult(
        accepted=result.deduped.count(False) if result else 0,
        deduplicated=result.deduped.count(True) if result else 0,
        last_offset=max(result.offsets) if result and result.offsets else None,
        quarantined=len(quarantined),
    )


# External consumers tail the durable log. Sending a frame does not ack it —
# the client commits the offsets it has handled. A grouped tail holds the
# consumer lease and renews it on each read; ``lease()`` itself rotates the
# fencing token, so the heartbeat goes through ``renew``. The read blocks
# until an append wakes it or the keepalive interval elapses. It does not poll.
_CONSUMER_LEASE_TTL_S = 30.0
_CONSUMER_KEEPALIVE_S = 15.0
_CONSUMER_BATCH = 100


@get("/streams/{name:str}/events", opt={"no_compress": True, "query_token": True})
async def stream_log_events(  # noqa: C901
    name: FromPath[str],
    state: State,
    request: Request,
    after: FromQuery[int | None] = None,
    group: FromQuery[str | None] = None,
) -> ServerSentEvent:
    """SSE tail of a durable stream log for external consumers.

    With no ``after`` and no ``Last-Event-ID``, a plain tail starts at the current
    head (live only). ``after=0`` replays from the first offset. A ``group`` takes
    the consumer lease and, unless a cursor was given, resumes from that group's
    committed offset. Delivery is at-least-once: frames are not auto-committed.
    """
    _log_name_or_404(state, name)
    raw_id = request.headers.get("Last-Event-ID")
    if raw_id:
        try:
            after = int(raw_id)
        except ValueError as exc:
            raise ClientException(detail="Last-Event-ID must be an integer offset") from exc

    group_name = group.strip() if group else ""
    held: Lease | None = None
    if group_name:
        held = await state.stream_log.lease(name, group_name, ttl=_CONSUMER_LEASE_TTL_S, owner=f"sse-{uuid4().hex}")
        if held is None:
            raise ClientException(status_code=409, detail=f"consumer group {group_name!r} on {name!r} is held")
    if held is not None and after is None:
        start = held.committed_offset
    elif after is not None:
        start = after
    else:
        start = await state.stream_log.head(name)

    async def tail() -> AsyncIterator[ServerSentEventMessage]:
        cursor = start
        try:
            # Comment frame first so EventSource onopen fires before any event.
            yield ServerSentEventMessage(comment="ok", data=None)
            if held is not None:
                yield ServerSentEventMessage(
                    event="lease",
                    data=json.dumps(
                        {"group": group_name, "token": held.token, "committed_offset": held.committed_offset}
                    ),
                )
            while True:
                # Renew on the same tick as the read. Idle tails wake every
                # keepalive interval, which is half the lease TTL.
                if held is not None and not await state.stream_log.renew(
                    name, group_name, held.token, ttl=_CONSUMER_LEASE_TTL_S
                ):
                    yield ServerSentEventMessage(event="error", data=json.dumps({"detail": "lease lost"}))
                    return
                batch = await state.stream_log.read(name, cursor, _CONSUMER_BATCH, wait=_CONSUMER_KEEPALIVE_S)
                if not batch:
                    yield ServerSentEventMessage(comment="keepalive", data=None)
                    continue
                for event in batch:
                    cursor = event.offset
                    yield ServerSentEventMessage(
                        id=event.offset,
                        data=json.dumps(
                            {
                                "offset": event.offset,
                                "ts": event.ts.isoformat(),
                                "payload": event.payload,
                                "idempotency_key": event.idempotency_key,
                                "headers": event.headers,
                            }
                        ),
                    )
        finally:
            if held is not None:
                with contextlib.suppress(Exception):
                    await asyncio.shield(state.stream_log.release(name, group_name, held.token))

    return ServerSentEvent(tail())


@post("/streams/{name:str}/commit", opt={"scope": "write"})
async def commit_stream(name: FromPath[str], data: StreamCommit, state: State) -> StreamCommitResult:
    """Ack a consumer group's offset. A stale fencing token is rejected (400)."""
    _log_name_or_404(state, name)
    await state.stream_log.commit(name, data.group, data.offset, data.token)
    return StreamCommitResult(group=data.group, committed_offset=data.offset)
