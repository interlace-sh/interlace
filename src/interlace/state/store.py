"""The state store — the OLTP control-plane database.

Persists snapshots, the interval ledger, and environment pointers (Phase 1; the
orchestrator/streaming tables land in later phases). SQLite (WAL) is the default
single-node backend; a Postgres backend implementing the same :class:`StateStore`
protocol is the scale-out swap. See docs/architecture/v2-design.md §6 for why this
is SQLite and not the analytical DuckDB engine.

SQLite is synchronous, so calls run in a worker thread; a lock serialises access
to the single shared connection (opened ``check_same_thread=False``). Schema
versioning uses ``PRAGMA user_version`` with an ordered migration list.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from interlace.ir.relation import TableRef
from interlace.state.interval import Interval, IntervalSet
from interlace.state.snapshot import ChangeCategory, Snapshot

_MIGRATIONS: list[str] = [
    # 0001 — snapshots, interval ledger, environment pointers
    """
    CREATE TABLE snapshots (
        name              TEXT NOT NULL,
        fingerprint       TEXT NOT NULL,
        metadata_hash     TEXT NOT NULL,
        physical_catalog  TEXT,
        physical_schema   TEXT NOT NULL,
        physical_name     TEXT NOT NULL,
        change_category   TEXT NOT NULL,
        created_at        TEXT NOT NULL,
        PRIMARY KEY (name, fingerprint)
    );

    CREATE TABLE intervals (
        name         TEXT NOT NULL,
        fingerprint  TEXT NOT NULL,
        start_ts     TEXT NOT NULL,
        end_ts       TEXT NOT NULL,
        PRIMARY KEY (name, fingerprint, start_ts, end_ts)
    );

    CREATE TABLE environments (
        environment  TEXT NOT NULL,
        model_name   TEXT NOT NULL,
        fingerprint  TEXT NOT NULL,
        promoted_at  TEXT NOT NULL,
        PRIMARY KEY (environment, model_name)
    );
    """,
]


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _snapshot_to_row(snapshot: Snapshot) -> tuple[str, str, str, str | None, str, str, str, str]:
    t = snapshot.physical_table
    return (
        snapshot.name,
        snapshot.fingerprint,
        snapshot.metadata_hash,
        t.catalog,
        t.schema,
        t.name,
        snapshot.change_category.value,
        _now_iso(),
    )


def _snapshot_from_row(row: sqlite3.Row, intervals: IntervalSet) -> Snapshot:
    return Snapshot(
        name=row["name"],
        fingerprint=row["fingerprint"],
        metadata_hash=row["metadata_hash"],
        physical_table=TableRef(
            schema=row["physical_schema"], name=row["physical_name"], catalog=row["physical_catalog"]
        ),
        change_category=ChangeCategory(row["change_category"]),
        intervals=intervals,
    )


def _intervals_from_rows(rows: Iterable[sqlite3.Row]) -> IntervalSet:
    return IntervalSet(
        Interval(datetime.fromisoformat(r["start_ts"]), datetime.fromisoformat(r["end_ts"])) for r in rows
    )


class StateStore(Protocol):
    """Backend-agnostic control-plane store. Implemented by SQLite now, Postgres later."""

    async def add_snapshot(self, snapshot: Snapshot) -> None: ...
    async def get_snapshot(self, name: str, fingerprint: str) -> Snapshot | None: ...
    async def list_snapshots(self, name: str) -> list[Snapshot]: ...
    async def record_interval(self, name: str, fingerprint: str, interval: Interval) -> None: ...
    async def get_intervals(self, name: str, fingerprint: str) -> IntervalSet: ...
    async def promote(self, environment: str, mapping: dict[str, str]) -> None: ...
    async def get_environment(self, environment: str) -> dict[str, str]: ...
    async def close(self) -> None: ...


class SqliteStateStore:
    """SQLite-backed :class:`StateStore` (WAL mode)."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._conn = connection
        self._lock = threading.Lock()

    @classmethod
    async def open(cls, path: str | Path) -> SqliteStateStore:
        connection = await asyncio.to_thread(cls._connect, str(path))
        return cls(connection)

    @staticmethod
    def _connect(path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA foreign_keys = ON")
        _migrate(conn)
        return conn

    async def close(self) -> None:
        await asyncio.to_thread(self._conn.close)

    # --- snapshots ----------------------------------------------------------

    async def add_snapshot(self, snapshot: Snapshot) -> None:
        await asyncio.to_thread(self._add_snapshot_sync, snapshot)

    def _add_snapshot_sync(self, snapshot: Snapshot) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO snapshots "
                "(name, fingerprint, metadata_hash, physical_catalog, physical_schema, physical_name, "
                " change_category, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                _snapshot_to_row(snapshot),
            )
            self._conn.execute(
                "DELETE FROM intervals WHERE name = ? AND fingerprint = ?",
                (snapshot.name, snapshot.fingerprint),
            )
            self._conn.executemany(
                "INSERT INTO intervals (name, fingerprint, start_ts, end_ts) VALUES (?, ?, ?, ?)",
                [
                    (snapshot.name, snapshot.fingerprint, iv.start.isoformat(), iv.end.isoformat())
                    for iv in snapshot.intervals
                ],
            )
            self._conn.commit()

    async def get_snapshot(self, name: str, fingerprint: str) -> Snapshot | None:
        return await asyncio.to_thread(self._get_snapshot_sync, name, fingerprint)

    def _get_snapshot_sync(self, name: str, fingerprint: str) -> Snapshot | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM snapshots WHERE name = ? AND fingerprint = ?", (name, fingerprint)
            ).fetchone()
            if row is None:
                return None
            interval_rows = self._conn.execute(
                "SELECT start_ts, end_ts FROM intervals WHERE name = ? AND fingerprint = ?", (name, fingerprint)
            ).fetchall()
        return _snapshot_from_row(row, _intervals_from_rows(interval_rows))

    async def list_snapshots(self, name: str) -> list[Snapshot]:
        return await asyncio.to_thread(self._list_snapshots_sync, name)

    def _list_snapshots_sync(self, name: str) -> list[Snapshot]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM snapshots WHERE name = ? ORDER BY created_at", (name,)).fetchall()
            result = []
            for row in rows:
                interval_rows = self._conn.execute(
                    "SELECT start_ts, end_ts FROM intervals WHERE name = ? AND fingerprint = ?",
                    (name, row["fingerprint"]),
                ).fetchall()
                result.append(_snapshot_from_row(row, _intervals_from_rows(interval_rows)))
        return result

    # --- interval ledger ----------------------------------------------------

    async def record_interval(self, name: str, fingerprint: str, interval: Interval) -> None:
        await asyncio.to_thread(self._record_interval_sync, name, fingerprint, interval)

    def _record_interval_sync(self, name: str, fingerprint: str, interval: Interval) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO intervals (name, fingerprint, start_ts, end_ts) VALUES (?, ?, ?, ?)",
                (name, fingerprint, interval.start.isoformat(), interval.end.isoformat()),
            )
            self._conn.commit()

    async def get_intervals(self, name: str, fingerprint: str) -> IntervalSet:
        return await asyncio.to_thread(self._get_intervals_sync, name, fingerprint)

    def _get_intervals_sync(self, name: str, fingerprint: str) -> IntervalSet:
        with self._lock:
            rows = self._conn.execute(
                "SELECT start_ts, end_ts FROM intervals WHERE name = ? AND fingerprint = ?", (name, fingerprint)
            ).fetchall()
        return _intervals_from_rows(rows)

    # --- environments -------------------------------------------------------

    async def promote(self, environment: str, mapping: dict[str, str]) -> None:
        await asyncio.to_thread(self._promote_sync, environment, mapping)

    def _promote_sync(self, environment: str, mapping: dict[str, str]) -> None:
        now = _now_iso()
        with self._lock:
            self._conn.executemany(
                "INSERT OR REPLACE INTO environments (environment, model_name, fingerprint, promoted_at) "
                "VALUES (?, ?, ?, ?)",
                [(environment, model, fingerprint, now) for model, fingerprint in mapping.items()],
            )
            self._conn.commit()

    async def get_environment(self, environment: str) -> dict[str, str]:
        return await asyncio.to_thread(self._get_environment_sync, environment)

    def _get_environment_sync(self, environment: str) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT model_name, fingerprint FROM environments WHERE environment = ?", (environment,)
            ).fetchall()
        return {row["model_name"]: row["fingerprint"] for row in rows}


def _migrate(conn: sqlite3.Connection) -> None:
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    for script in _MIGRATIONS[version:]:
        conn.executescript(script)
    conn.execute(f"PRAGMA user_version = {len(_MIGRATIONS)}")
    conn.commit()
