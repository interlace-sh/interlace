"""The durable stream log contract.

This is the heart of the Cloudflare-style ingestion story and the one piece v0.x
got most wrong (in-memory queues, lost on restart). ``append`` must not return
before events are durable, so a 200-OK means "fsynced". Consumer offsets are
committed transactionally with fencing tokens, which kills the read/ack race by
construction. Idempotency keys give effectively-exactly-once.

The default backend is SQLite (WAL, group commit); Postgres/Redpanda/NATS are
optional backends behind this same Protocol.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True)
class Event:
    """An inbound event to append. ``idempotency_key`` enables dedup on append."""

    payload: dict[str, Any]
    idempotency_key: str | None = None
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class StoredEvent:
    """A durably-stored event with its assigned per-stream offset."""

    offset: int
    ts: datetime
    payload: dict[str, Any]
    idempotency_key: str | None = None
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class AppendResult:
    """Outcome of an append: assigned offsets and which entries were deduped."""

    offsets: list[int]
    deduped: list[bool]


@dataclass(frozen=True)
class Lease:
    """A consumer-group lease: where to resume, plus the fencing token for commits."""

    committed_offset: int
    token: str


class StreamLog(Protocol):
    """Durable, ordered, replayable per-stream log with at-least-once delivery."""

    async def append(self, stream: str, events: list[Event]) -> AppendResult:
        """Durably append events. Must fsync before returning. Raises ``Backpressure`` when full."""
        ...

    async def read(self, stream: str, after_offset: int, limit: int, wait: float | None = None) -> list[StoredEvent]:
        """Read up to ``limit`` events after ``after_offset``; optionally long-poll for ``wait`` seconds."""
        ...

    async def lease(self, stream: str, group: str, *, ttl: float, owner: str) -> Lease | None:
        """Acquire the single active lease for ``(stream, group)``, or ``None`` if held."""
        ...

    async def commit(self, stream: str, group: str, offset: int, lease_token: str) -> None:
        """Commit a consumer offset atomically; rejects a stale fencing token."""
        ...

    async def trim(self, stream: str, *, before_offset: int | None = None, before_ts: datetime | None = None) -> int:
        """Apply retention; returns the number of events removed."""
        ...
