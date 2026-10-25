"""One process-wide DuckDB for Python models that read outside the warehouse.

A model that calls ``duckdb.connect()`` gets DuckDB's defaults: 80% of RAM and a
thread per core, once per model. Closing the instance does not return that
allocator arena, so a parallel build keeps one residue per model. This database
is opened once. Queries take a lock, so two raw scans cannot add their buffer
pools. The cap is the active engine's ``memory_limit`` when a model run has
published it, otherwise 1 GiB. Threads stay at 2 or the engine's count when that
is lower. Spill goes to a directory of its own: DuckDB deletes every spill file
in its temp directory when that instance closes, and sharing the warehouse's
directory would let one close remove the other's files.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path

import duckdb

_LOCK = threading.Lock()
_connection: duckdb.DuckDBPyConnection | None = None
_applied: ScratchLimits | None = None
_active: ContextVar[ScratchLimits | None] = ContextVar("interlace_scratch_limits", default=None)


@dataclass(frozen=True)
class ScratchLimits:
    """The instance caps for the shared scratch database."""

    memory_limit: str = "1GB"
    threads: int = 2
    temp_directory: str = ""


def limits_for(engine: object) -> ScratchLimits:
    """Caps to publish for ``engine``.

    ``memory_limit`` and a temp directory come from the engine settings when it
    has them. Threads are at most 2: a scratch scan does not need a thread per
    core, and each thread keeps its own buffers inside the cap.
    """
    raw = getattr(engine, "settings", None)
    settings: Mapping[str, object] = raw if isinstance(raw, Mapping) else {}
    memory = settings.get("memory_limit", "1GB")
    base = settings.get("temp_directory")
    if isinstance(base, str) and base:
        temp = str(Path(base) / "scratch")
    else:
        temp = str(Path.cwd() / ".tmp" / "scratch")
    return ScratchLimits(memory_limit=str(memory), threads=_threads(settings.get("threads")), temp_directory=temp)


@contextmanager
def bind(engine: object) -> Iterator[None]:
    """Publish ``engine``'s caps for :func:`session` calls on this task.

    ``asyncio.to_thread`` copies the context, so a sync read inside an async
    model still sees them. Opening the database waits until something asks.
    """
    token = _active.set(limits_for(engine))
    try:
        yield
    finally:
        _active.reset(token)


@contextmanager
def session() -> Iterator[duckdb.DuckDBPyConnection]:
    """Borrow the scratch database. Do not close it.

    The lock is held for the block, and the cursor is closed on the way out.
    The database stays open for the process.
    """
    limits = _active.get() or ScratchLimits(temp_directory=str(Path.cwd() / ".tmp" / "scratch"))
    with _LOCK:
        connection = _ensure(limits)
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()


def reset() -> None:
    """Close the scratch database. Tests use this so caps do not leak between cases."""
    global _connection, _applied
    with _LOCK:
        if _connection is not None:
            _connection.close()
        _connection = None
        _applied = None


def _threads(value: object) -> int:
    if isinstance(value, bool) or value is None:
        return 2
    try:
        parsed = int(str(value))
    except ValueError:
        return 2
    return max(1, min(parsed, 2))


def _ensure(limits: ScratchLimits) -> duckdb.DuckDBPyConnection:
    global _connection, _applied
    if _connection is None:
        directory = Path(limits.temp_directory)
        directory.mkdir(parents=True, exist_ok=True)
        _connection = duckdb.connect(
            ":memory:",
            config={
                "memory_limit": limits.memory_limit,
                "threads": str(limits.threads),
                "temp_directory": str(directory),
            },
        )
        _applied = limits
        return _connection
    if limits != _applied:
        _apply(_connection, limits)
        _applied = limits
    return _connection


def _apply(connection: duckdb.DuckDBPyConnection, limits: ScratchLimits) -> None:
    directory = Path(limits.temp_directory)
    directory.mkdir(parents=True, exist_ok=True)
    # SET is session-scoped and does not reach cursors. GLOBAL does.
    connection.execute(f"SET GLOBAL memory_limit = {_quote(limits.memory_limit)}")
    connection.execute(f"SET GLOBAL threads = {limits.threads}")
    connection.execute(f"SET GLOBAL temp_directory = {_quote(str(directory))}")


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
