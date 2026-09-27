"""One SQLite connection shared by every control-plane store."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from collections.abc import Callable
from typing import Any, TypeVar

from interlace.state.schema import _migrate

_R = TypeVar("_R")


class ControlDb:
    """The single WAL connection. Stores take this; they do not open their own."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.conn = connection
        self.lock = threading.Lock()

    @staticmethod
    def connect(path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")  # CLI + daemon share this file: wait, don't error
        conn.execute("PRAGMA cache_size = -65536")
        _migrate(conn)
        return conn

    async def io(self, fn: Callable[..., _R], /, *args: Any, **kwargs: Any) -> _R:
        """The one worker-thread entry. Sync methods keep the connection lock."""
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def close(self) -> None:
        await self.io(self.conn.close)
