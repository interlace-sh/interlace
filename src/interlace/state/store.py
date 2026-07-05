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
import hashlib
import json
import secrets
import sqlite3
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, TypedDict

from interlace.ir.relation import TableRef
from interlace.state.interval import Interval, IntervalSet
from interlace.state.snapshot import ChangeCategory, Snapshot

_MIGRATIONS: list[str] = [
    # 0001 — snapshots, interval ledger, environment pointers
    """
    CREATE TABLE snapshots (
        name               TEXT NOT NULL,
        fingerprint        TEXT NOT NULL,
        local_fingerprint  TEXT NOT NULL,
        metadata_hash      TEXT NOT NULL,
        definition_sql     TEXT,
        physical_catalog   TEXT,
        physical_schema    TEXT NOT NULL,
        physical_name      TEXT NOT NULL,
        change_category    TEXT NOT NULL,
        created_at         TEXT NOT NULL,
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
    # 0002 — orchestration: durable run queue + per-trigger state
    """
    CREATE TABLE work_queue (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        idempotency_key  TEXT UNIQUE,
        flow_selector    TEXT NOT NULL,
        partition_start  TEXT,
        partition_end    TEXT,
        priority         INTEGER NOT NULL DEFAULT 0,
        state            TEXT NOT NULL DEFAULT 'queued',
        attempts         INTEGER NOT NULL DEFAULT 0,
        error            TEXT,
        enqueued_at      TEXT NOT NULL
    );

    CREATE TABLE trigger_state (
        trigger_id     TEXT PRIMARY KEY,
        last_fired_at  TEXT
    );
    """,
    # 0003 — durable event log (run/stream lifecycle; SSE replay spine)
    """
    CREATE TABLE event_log (
        seq      INTEGER PRIMARY KEY AUTOINCREMENT,
        ts       TEXT NOT NULL,
        type     TEXT NOT NULL,
        entity   TEXT,
        payload  TEXT
    );
    """,
    # 0004 — API keys (scoped) for the HTTP service
    """
    CREATE TABLE api_keys (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT NOT NULL,
        key_hash    TEXT NOT NULL UNIQUE,
        scopes      TEXT NOT NULL,
        created_at  TEXT NOT NULL
    );
    """,
    # 0005 — data-quality check results (gate promotion; surfaced via API/UI)
    """
    CREATE TABLE check_results (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        environment  TEXT NOT NULL,
        model        TEXT NOT NULL,
        fingerprint  TEXT NOT NULL,
        check_name   TEXT NOT NULL,
        check_type   TEXT NOT NULL,
        severity     TEXT NOT NULL,
        status       TEXT NOT NULL,
        failures     INTEGER NOT NULL DEFAULT 0,
        message      TEXT,
        executed_at  TEXT NOT NULL
    );
    CREATE INDEX idx_check_results_model ON check_results (model, id DESC);
    """,
]


class RunRecord(TypedDict):
    """A work-queue row as returned by ``list_runs`` / ``get_run``."""

    id: int
    idempotency_key: str | None
    flow_selector: list[str]
    partition_start: str | None
    partition_end: str | None
    priority: int
    state: str
    attempts: int
    error: str | None
    enqueued_at: str | None


@dataclass
class QueuedRun:
    """A claimed run from the work queue."""

    id: int
    flow_selector: list[str]
    partition_start: str | None
    partition_end: str | None
    priority: int
    attempts: int


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _snapshot_to_row(snapshot: Snapshot) -> tuple[str, str, str, str, str | None, str | None, str, str, str, str]:
    t = snapshot.physical_table
    return (
        snapshot.name,
        snapshot.fingerprint,
        snapshot.local_fingerprint,
        snapshot.metadata_hash,
        snapshot.definition_sql,
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
        local_fingerprint=row["local_fingerprint"],
        definition_sql=row["definition_sql"],
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
    async def record_check_results(self, environment: str, fingerprint: str, outcomes: Iterable[Any]) -> None: ...
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
                "(name, fingerprint, local_fingerprint, metadata_hash, definition_sql, physical_catalog, "
                " physical_schema, physical_name, change_category, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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

    # --- garbage collection ---------------------------------------------------

    async def referenced_snapshots(self) -> set[tuple[str, str]]:
        """Every ``(model, fingerprint)`` some environment currently points at."""
        return await asyncio.to_thread(self._referenced_snapshots_sync)

    def _referenced_snapshots_sync(self) -> set[tuple[str, str]]:
        with self._lock:
            rows = self._conn.execute("SELECT DISTINCT model_name, fingerprint FROM environments").fetchall()
        return {(row["model_name"], row["fingerprint"]) for row in rows}

    async def list_snapshot_rows(self) -> list[dict[str, str]]:
        """Every snapshot row (no intervals): name, fingerprint, physical table, created_at."""
        return await asyncio.to_thread(self._list_snapshot_rows_sync)

    def _list_snapshot_rows_sync(self) -> list[dict[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT name, fingerprint, physical_schema, physical_name, created_at FROM snapshots"
            ).fetchall()
        return [dict(row) for row in rows]

    async def delete_snapshots(self, pairs: Iterable[tuple[str, str]]) -> None:
        """Remove snapshot rows and their interval-ledger entries."""
        await asyncio.to_thread(self._delete_snapshots_sync, list(pairs))

    def _delete_snapshots_sync(self, pairs: list[tuple[str, str]]) -> None:
        with self._lock:
            self._conn.executemany("DELETE FROM snapshots WHERE name = ? AND fingerprint = ?", pairs)
            self._conn.executemany("DELETE FROM intervals WHERE name = ? AND fingerprint = ?", pairs)
            self._conn.commit()

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

    async def list_environments(self) -> list[str]:
        return await asyncio.to_thread(self._list_environments_sync)

    def _list_environments_sync(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute("SELECT DISTINCT environment FROM environments ORDER BY environment").fetchall()
        return [row["environment"] for row in rows]

    # --- work queue ---------------------------------------------------------

    async def enqueue_run(
        self, idempotency_key: str, flow_selector: list[str], partition: tuple[str, str] | None, priority: int = 0
    ) -> bool:
        """Enqueue a run; returns False if an identical idempotency key is already queued."""
        return await asyncio.to_thread(self._enqueue_run_sync, idempotency_key, flow_selector, partition, priority)

    def _enqueue_run_sync(
        self, idempotency_key: str, flow_selector: list[str], partition: tuple[str, str] | None, priority: int
    ) -> bool:
        with self._lock:
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO work_queue "
                "(idempotency_key, flow_selector, partition_start, partition_end, priority, enqueued_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    idempotency_key or None,
                    json.dumps(flow_selector),
                    partition[0] if partition else None,
                    partition[1] if partition else None,
                    priority,
                    _now_iso(),
                ),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    async def claim_runs(self, limit: int = 10) -> list[QueuedRun]:
        return await asyncio.to_thread(self._claim_runs_sync, limit)

    def _claim_runs_sync(self, limit: int) -> list[QueuedRun]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            rows = self._conn.execute(
                "SELECT id, flow_selector, partition_start, partition_end, priority, attempts "
                "FROM work_queue WHERE state = 'queued' ORDER BY priority DESC, id LIMIT ?",
                (limit,),
            ).fetchall()
            for row in rows:
                self._conn.execute(
                    "UPDATE work_queue SET state = 'running', attempts = attempts + 1 WHERE id = ?", (row["id"],)
                )
            self._conn.commit()
        return [
            QueuedRun(
                id=row["id"],
                flow_selector=json.loads(row["flow_selector"]),
                partition_start=row["partition_start"],
                partition_end=row["partition_end"],
                priority=row["priority"],
                attempts=row["attempts"] + 1,
            )
            for row in rows
        ]

    async def finish_run(self, run_id: int, *, success: bool, error: str | None = None) -> None:
        await asyncio.to_thread(self._finish_run_sync, run_id, success, error)

    def _finish_run_sync(self, run_id: int, success: bool, error: str | None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE work_queue SET state = ?, error = ? WHERE id = ?",
                ("succeeded" if success else "failed", error, run_id),
            )
            self._conn.commit()

    async def count_pending_runs(self) -> int:
        return await asyncio.to_thread(self._count_pending_runs_sync)

    def _count_pending_runs_sync(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT count(*) FROM work_queue WHERE state IN ('queued', 'running')").fetchone()
        return int(row[0])

    _RUN_COLUMNS = (
        "id, idempotency_key, flow_selector, partition_start, partition_end, "
        "priority, state, attempts, error, enqueued_at"
    )

    @staticmethod
    def _run_dict(row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            flow_selector=json.loads(row["flow_selector"]),
            partition_start=row["partition_start"],
            partition_end=row["partition_end"],
            priority=row["priority"],
            state=row["state"],
            attempts=row["attempts"],
            error=row["error"],
            enqueued_at=row["enqueued_at"],
        )

    async def list_runs(self, limit: int = 50) -> list[RunRecord]:
        return await asyncio.to_thread(self._list_runs_sync, limit)

    def _list_runs_sync(self, limit: int) -> list[RunRecord]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {self._RUN_COLUMNS} FROM work_queue ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._run_dict(row) for row in rows]

    async def get_run(self, run_id: int) -> RunRecord | None:
        return await asyncio.to_thread(self._get_run_sync, run_id)

    def _get_run_sync(self, run_id: int) -> RunRecord | None:
        with self._lock:
            row = self._conn.execute(f"SELECT {self._RUN_COLUMNS} FROM work_queue WHERE id = ?", (run_id,)).fetchone()
        return self._run_dict(row) if row else None

    async def events_for_entity(self, entity: str) -> list[dict[str, object]]:
        return await asyncio.to_thread(self._events_for_entity_sync, entity)

    def _events_for_entity_sync(self, entity: str) -> list[dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, ts, type, entity, payload FROM event_log WHERE entity = ? ORDER BY seq", (entity,)
            ).fetchall()
        return [
            {
                "seq": row["seq"],
                "ts": row["ts"],
                "type": row["type"],
                "entity": row["entity"],
                "payload": json.loads(row["payload"]) if row["payload"] else None,
            }
            for row in rows
        ]

    # --- event log ----------------------------------------------------------

    async def append_event(self, type: str, entity: str | None = None, payload: dict[str, object] | None = None) -> int:
        return await asyncio.to_thread(self._append_event_sync, type, entity, payload)

    def _append_event_sync(self, type: str, entity: str | None, payload: dict[str, object] | None) -> int:
        with self._lock:
            cursor = self._conn.execute(
                "INSERT INTO event_log (ts, type, entity, payload) VALUES (?, ?, ?, ?)",
                (_now_iso(), type, entity, json.dumps(payload) if payload is not None else None),
            )
            self._conn.commit()
            return int(cursor.lastrowid or 0)

    async def read_events(self, after_seq: int = 0, limit: int = 200) -> list[dict[str, object]]:
        return await asyncio.to_thread(self._read_events_sync, after_seq, limit)

    def _read_events_sync(self, after_seq: int, limit: int) -> list[dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, ts, type, entity, payload FROM event_log WHERE seq > ? ORDER BY seq LIMIT ?",
                (after_seq, limit),
            ).fetchall()
        return [
            {
                "seq": row["seq"],
                "ts": row["ts"],
                "type": row["type"],
                "entity": row["entity"],
                "payload": json.loads(row["payload"]) if row["payload"] else None,
            }
            for row in rows
        ]

    # --- API keys -----------------------------------------------------------

    async def create_api_key(self, name: str, scopes: list[str]) -> str:
        """Create a key; returns the plaintext token (shown once — only the hash is stored)."""
        return await asyncio.to_thread(self._create_api_key_sync, name, scopes)

    def _create_api_key_sync(self, name: str, scopes: list[str]) -> str:
        token = "ilk_" + secrets.token_hex(16)
        with self._lock:
            self._conn.execute(
                "INSERT INTO api_keys (name, key_hash, scopes, created_at) VALUES (?, ?, ?, ?)",
                (name, hashlib.sha256(token.encode()).hexdigest(), json.dumps(scopes), _now_iso()),
            )
            self._conn.commit()
        return token

    async def verify_api_key(self, token: str) -> list[str] | None:
        """Return the key's scopes, or None if the token is unknown."""
        return await asyncio.to_thread(self._verify_api_key_sync, token)

    def _verify_api_key_sync(self, token: str) -> list[str] | None:
        digest = hashlib.sha256(token.encode()).hexdigest()
        with self._lock:
            row = self._conn.execute("SELECT scopes FROM api_keys WHERE key_hash = ?", (digest,)).fetchone()
        return json.loads(row["scopes"]) if row else None

    async def count_api_keys(self) -> int:
        return await asyncio.to_thread(self._count_api_keys_sync)

    def _count_api_keys_sync(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT count(*) FROM api_keys").fetchone()
        return int(row[0])

    async def list_api_keys(self) -> list[dict[str, object]]:
        return await asyncio.to_thread(self._list_api_keys_sync)

    def _list_api_keys_sync(self) -> list[dict[str, object]]:
        with self._lock:
            rows = self._conn.execute("SELECT name, scopes, created_at FROM api_keys ORDER BY id").fetchall()
        return [{"name": r["name"], "scopes": json.loads(r["scopes"]), "created_at": r["created_at"]} for r in rows]

    # --- check results ------------------------------------------------------

    async def record_check_results(self, environment: str, fingerprint: str, outcomes: Iterable[Any]) -> None:
        """Persist one model's check outcomes (objects with name/type/severity/status/failures/message)."""
        await asyncio.to_thread(self._record_check_results_sync, environment, fingerprint, list(outcomes))

    def _record_check_results_sync(self, environment: str, fingerprint: str, outcomes: list[Any]) -> None:
        now = _now_iso()
        with self._lock:
            self._conn.executemany(
                "INSERT INTO check_results (environment, model, fingerprint, check_name, check_type, severity, "
                "status, failures, message, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        environment,
                        o.model,
                        fingerprint,
                        o.name,
                        o.type,
                        o.severity,
                        o.status,
                        o.failures,
                        o.message,
                        now,
                    )
                    for o in outcomes
                ],
            )
            self._conn.commit()

    async def list_check_results(self, model: str | None = None, limit: int = 200) -> list[dict[str, object]]:
        return await asyncio.to_thread(self._list_check_results_sync, model, limit)

    def _list_check_results_sync(self, model: str | None, limit: int) -> list[dict[str, object]]:
        sql = (
            "SELECT id, environment, model, fingerprint, check_name, check_type, severity, status, failures, "
            "message, executed_at FROM check_results"
        )
        params: list[object] = []
        if model is not None:
            sql += " WHERE model = ?"
            params.append(model)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    # --- trigger state ------------------------------------------------------

    async def get_trigger_last_fired(self, trigger_id: str) -> datetime | None:
        return await asyncio.to_thread(self._get_trigger_last_fired_sync, trigger_id)

    def _get_trigger_last_fired_sync(self, trigger_id: str) -> datetime | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT last_fired_at FROM trigger_state WHERE trigger_id = ?", (trigger_id,)
            ).fetchone()
        return datetime.fromisoformat(row["last_fired_at"]) if row and row["last_fired_at"] else None

    async def set_trigger_last_fired(self, trigger_id: str, when: datetime) -> None:
        await asyncio.to_thread(self._set_trigger_last_fired_sync, trigger_id, when)

    def _set_trigger_last_fired_sync(self, trigger_id: str, when: datetime) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO trigger_state (trigger_id, last_fired_at) VALUES (?, ?)",
                (trigger_id, when.isoformat()),
            )
            self._conn.commit()


def _migrate(conn: sqlite3.Connection) -> None:
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    for script in _MIGRATIONS[version:]:
        conn.executescript(script)
    conn.execute(f"PRAGMA user_version = {len(_MIGRATIONS)}")
    conn.commit()
