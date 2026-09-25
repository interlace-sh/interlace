"""Append decoded changes to a stream, and confirm their LSN only after a flush."""

from __future__ import annotations

from interlace.cdc.decode import Change, payload_of
from interlace.state.store import SqliteStateStore
from interlace.streaming.log import Event, StreamLog


async def publish_changes(log: StreamLog, store: SqliteStateStore, stream: str, changes: list[Change]) -> list[int]:
    """Append ``changes`` to ``stream``. A repeated LSN is deduped by the log.

    The LSN is recorded against the log offset and is not confirmed here.
    """
    if not changes:
        return []
    result = await log.append(
        stream,
        [Event(payload=payload_of(change), idempotency_key=f"cdc:{change.lsn}") for change in changes],
    )
    await store.cdc_note_pending(stream, list(zip(result.offsets, (change.lsn for change in changes), strict=True)))
    return result.offsets


async def confirm_flushed(store: SqliteStateStore, stream: str, watermark: int) -> str | None:
    """Advance the stored LSN up to the offset ``flush_streams`` has committed."""
    return await store.cdc_advance(stream, watermark)
