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

import asyncio
import json
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from interlace.exceptions import StreamError


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


_SCHEMA = """
CREATE TABLE IF NOT EXISTS stream_events (
    stream   TEXT NOT NULL,
    offset   INTEGER NOT NULL,
    ts       TEXT NOT NULL,
    key      TEXT,
    headers  TEXT,
    payload  TEXT NOT NULL,
    PRIMARY KEY (stream, offset)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_stream_events_key
    ON stream_events (stream, key) WHERE key IS NOT NULL;
CREATE TABLE IF NOT EXISTS stream_heads (
    stream       TEXT PRIMARY KEY,
    next_offset  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS consumer_state (
    stream            TEXT NOT NULL,
    grp               TEXT NOT NULL,
    committed_offset  INTEGER NOT NULL DEFAULT 0,
    lease_owner       TEXT,
    lease_token       TEXT,
    lease_expires_at  TEXT,
    PRIMARY KEY (stream, grp)
);
"""


class SqliteStreamLog:
    """SQLite (WAL) :class:`StreamLog` — the single-node default backend.

    Offsets start at 1 per stream; ``committed_offset`` 0 means "from the
    beginning". Appends commit (fsync under WAL semantics) before returning, so
    a 200-OK from the publish endpoint means durable. Dedup by idempotency key
    is a partial unique index — transactional by construction.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._conn = connection
        self._lock = threading.Lock()

    @classmethod
    async def open(cls, path: str | Path) -> SqliteStreamLog:
        return cls(await asyncio.to_thread(cls._connect, str(path)))

    @staticmethod
    def _connect(path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.executescript(_SCHEMA)
        conn.commit()
        return conn

    async def close(self) -> None:
        await asyncio.to_thread(self._conn.close)

    # --- producer -----------------------------------------------------------

    async def append(self, stream: str, events: list[Event]) -> AppendResult:
        return await asyncio.to_thread(self._append_sync, stream, events)

    def _append_sync(self, stream: str, events: list[Event]) -> AppendResult:
        now = datetime.now(UTC).isoformat()
        offsets: list[int] = []
        deduped: list[bool] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute("SELECT next_offset FROM stream_heads WHERE stream = ?", (stream,)).fetchone()
            offset = int(row["next_offset"]) if row else 1
            for event in events:
                cursor = self._conn.execute(
                    "INSERT OR IGNORE INTO stream_events (stream, offset, ts, key, headers, payload) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        stream,
                        offset,
                        now,
                        event.idempotency_key,
                        json.dumps(event.headers) if event.headers else None,
                        json.dumps(event.payload),
                    ),
                )
                if cursor.rowcount:
                    offsets.append(offset)
                    deduped.append(False)
                    offset += 1
                else:  # idempotency key already seen: report its original offset
                    seen = self._conn.execute(
                        "SELECT offset FROM stream_events WHERE stream = ? AND key = ?",
                        (stream, event.idempotency_key),
                    ).fetchone()
                    offsets.append(int(seen["offset"]) if seen else 0)
                    deduped.append(True)
            self._conn.execute(
                "INSERT OR REPLACE INTO stream_heads (stream, next_offset) VALUES (?, ?)", (stream, offset)
            )
            self._conn.commit()  # durable before the publisher sees 200
        return AppendResult(offsets=offsets, deduped=deduped)

    # --- consumers ----------------------------------------------------------

    async def read(self, stream: str, after_offset: int, limit: int, wait: float | None = None) -> list[StoredEvent]:
        deadline = asyncio.get_event_loop().time() + wait if wait else None
        while True:
            events = await asyncio.to_thread(self._read_sync, stream, after_offset, limit)
            if events or deadline is None or asyncio.get_event_loop().time() >= deadline:
                return events
            await asyncio.sleep(0.05)  # long-poll: cheap WAL reads until data or deadline

    def _read_sync(self, stream: str, after_offset: int, limit: int) -> list[StoredEvent]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT offset, ts, key, headers, payload FROM stream_events "
                "WHERE stream = ? AND offset > ? ORDER BY offset LIMIT ?",
                (stream, after_offset, limit),
            ).fetchall()
        return [
            StoredEvent(
                offset=r["offset"],
                ts=datetime.fromisoformat(r["ts"]),
                payload=json.loads(r["payload"]),
                idempotency_key=r["key"],
                headers=json.loads(r["headers"]) if r["headers"] else {},
            )
            for r in rows
        ]

    async def head(self, stream: str) -> int:
        """The highest offset ever assigned (0 = empty stream)."""
        return await asyncio.to_thread(self._head_sync, stream)

    def _head_sync(self, stream: str) -> int:
        with self._lock:
            row = self._conn.execute("SELECT next_offset FROM stream_heads WHERE stream = ?", (stream,)).fetchone()
        return int(row["next_offset"]) - 1 if row else 0

    async def lease(self, stream: str, group: str, *, ttl: float, owner: str) -> Lease | None:
        return await asyncio.to_thread(self._lease_sync, stream, group, ttl, owner)

    def _lease_sync(self, stream: str, group: str, ttl: float, owner: str) -> Lease | None:
        now = datetime.now(UTC)
        expires = (now + timedelta(seconds=ttl)).isoformat()
        token = uuid4().hex
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT committed_offset, lease_owner, lease_expires_at FROM consumer_state "
                "WHERE stream = ? AND grp = ?",
                (stream, group),
            ).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO consumer_state (stream, grp, committed_offset, lease_owner, lease_token, "
                    "lease_expires_at) VALUES (?, ?, 0, ?, ?, ?)",
                    (stream, group, owner, token, expires),
                )
                self._conn.commit()
                return Lease(committed_offset=0, token=token)
            held = (
                row["lease_owner"] is not None
                and row["lease_owner"] != owner
                and row["lease_expires_at"] is not None
                and datetime.fromisoformat(row["lease_expires_at"]) > now
            )
            if held:
                self._conn.commit()
                return None
            self._conn.execute(
                "UPDATE consumer_state SET lease_owner = ?, lease_token = ?, lease_expires_at = ? "
                "WHERE stream = ? AND grp = ?",
                (owner, token, expires, stream, group),
            )
            self._conn.commit()
            return Lease(committed_offset=int(row["committed_offset"]), token=token)

    async def commit(self, stream: str, group: str, offset: int, lease_token: str) -> None:
        await asyncio.to_thread(self._commit_sync, stream, group, offset, lease_token)

    def _commit_sync(self, stream: str, group: str, offset: int, lease_token: str) -> None:
        with self._lock:
            cursor = self._conn.execute(
                "UPDATE consumer_state SET committed_offset = ? WHERE stream = ? AND grp = ? AND lease_token = ?",
                (offset, stream, group, lease_token),
            )
            self._conn.commit()
        if cursor.rowcount == 0:
            raise StreamError(f"stale lease token for {stream!r} group {group!r}; another consumer holds the lease")

    async def trim(self, stream: str, *, before_offset: int | None = None, before_ts: datetime | None = None) -> int:
        return await asyncio.to_thread(self._trim_sync, stream, before_offset, before_ts)

    def _trim_sync(self, stream: str, before_offset: int | None, before_ts: datetime | None) -> int:
        clauses: list[str] = ["stream = ?"]
        params: list[object] = [stream]
        if before_offset is not None:
            clauses.append("offset < ?")
            params.append(before_offset)
        if before_ts is not None:
            clauses.append("ts < ?")
            params.append(before_ts.isoformat())
        if len(clauses) == 1:
            return 0  # refuse to trim everything by accident
        with self._lock:
            cursor = self._conn.execute(
                f"DELETE FROM stream_events WHERE {' AND '.join(clauses)}", params
            )  # noqa: S608
            self._conn.commit()
        return int(cursor.rowcount)
