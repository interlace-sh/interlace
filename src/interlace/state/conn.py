"""One connection shared by every control-plane store.

SQLite is the default. ``connect_postgres`` opens the same stores on Postgres.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from collections.abc import Callable, Sequence
from typing import Any, Protocol, TypeVar

from interlace.state.schema import _migrate

_R = TypeVar("_R")


class _Cursor(Protocol):
    lastrowid: int
    rowcount: int

    def fetchone(self) -> Any: ...

    def fetchall(self) -> Sequence[Any]: ...


class ConnLike(Protocol):
    """The slice of ``sqlite3.Connection`` the stores use. Postgres implements it too."""

    def execute(self, sql: str, parameters: Any = ..., /) -> _Cursor: ...

    def executemany(self, sql: str, seq_of_parameters: Any, /) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class ControlDb:
    """The single control-plane connection. Stores take this; they do not open their own.

    The default is a SQLite file. ``connect_postgres`` is the same stores on a
    Postgres schema (``state_url``).
    """

    def __init__(self, connection: ConnLike, *, dialect: str = "sqlite") -> None:
        self.conn = connection
        self.lock = threading.Lock()
        self.dialect = dialect

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

    @staticmethod
    def connect_postgres(dsn: str, schema: str = "interlace") -> ConnLike:
        from interlace.state.pg import connect, migrate

        conn = connect(dsn, schema)
        try:
            migrate(conn, schema)
        except Exception:
            conn.close()
            raise
        return conn

    async def io(self, fn: Callable[..., _R], /, *args: Any, **kwargs: Any) -> _R:
        """The one worker-thread entry. Sync methods keep the connection lock."""
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def close(self) -> None:
        await self.io(self.conn.close)
