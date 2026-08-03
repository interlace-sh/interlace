"""The state store — the OLTP control-plane database.

Owns every non-warehouse table: snapshots, the interval ledger, environment
pointers and promotion history, the durable run queue, per-trigger state, the
event log, API keys, and check results. SQLite (WAL) is the single-node backend.
See docs/architecture/architecture.md §6 for why this is SQLite and not the
analytical DuckDB engine.

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
from datetime import UTC, datetime, timedelta
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
    # 0006 — multi-engine: which named engine owns each snapshot's physical table
    """
    ALTER TABLE snapshots ADD COLUMN engine TEXT NOT NULL DEFAULT 'default';
    """,
    # 0007 — per-task worker: leases (crash reclaim), cooperative cancellation
    """
    ALTER TABLE work_queue ADD COLUMN lease_owner TEXT;
    ALTER TABLE work_queue ADD COLUMN lease_expires_at TEXT;
    ALTER TABLE work_queue ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0;
    """,
    # 0008 — indexes for the two hot growing tables (claim scans, run timelines)
    """
    CREATE INDEX idx_work_queue_state ON work_queue (state, priority DESC, id);
    CREATE INDEX idx_event_log_entity ON event_log (entity);
    """,
    # 0009 — restate runs: reprocess the window instead of catching up
    """
    ALTER TABLE work_queue ADD COLUMN restate INTEGER NOT NULL DEFAULT 0;
    """,
    # 0010 — promotion history: every promote snapshots the environment's FULL
    # mapping as one generation, so `rollback` can repoint views to any earlier
    # state (as long as gc hasn't reclaimed those snapshots)
    """
    CREATE TABLE promotion_history (
        environment  TEXT NOT NULL,
        generation   INTEGER NOT NULL,
        model_name   TEXT NOT NULL,
        fingerprint  TEXT NOT NULL,
        promoted_at  TEXT NOT NULL,
        PRIMARY KEY (environment, generation, model_name)
    );
    CREATE INDEX idx_promotion_history_env ON promotion_history (environment, generation DESC);
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
    restate: bool


@dataclass
class QueuedRun:
    """A claimed run from the work queue."""

    id: int
    flow_selector: list[str]
    partition_start: str | None
    partition_end: str | None
    priority: int
    attempts: int
    restate: bool = False


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _snapshot_to_row(
    snapshot: Snapshot,
) -> tuple[str, str, str, str, str | None, str | None, str, str, str, str, str]:
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
        snapshot.engine,
    )


def _snapshot_from_row(row: sqlite3.Row, intervals: IntervalSet) -> Snapshot:
    keys = row.keys()
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
        engine=row["engine"] if "engine" in keys else "default",
    )


def _intervals_from_rows(rows: Iterable[sqlite3.Row]) -> IntervalSet:
    return IntervalSet(
        Interval(datetime.fromisoformat(r["start_ts"]), datetime.fromisoformat(r["end_ts"])) for r in rows
    )


class StateStore(Protocol):
    """The plan/apply slice of the control-plane store — the surface differ and
    apply depend on. ``SqliteStateStore`` is the implementation and carries the
    full surface (work queue, event log, api keys, ...); this Protocol is
    deliberately narrow so the core stays decoupled from the daemon's tables."""

    async def add_snapshot(self, snapshot: Snapshot) -> None: ...
    async def get_snapshot(self, name: str, fingerprint: str) -> Snapshot | None: ...
    async def get_snapshots(self, pairs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], Snapshot]: ...
    async def list_snapshots(self, name: str) -> list[Snapshot]: ...
    async def record_interval(self, name: str, fingerprint: str, interval: Interval) -> None: ...
    async def get_intervals(self, name: str, fingerprint: str) -> IntervalSet: ...
    async def promote(self, environment: str, mapping: dict[str, str]) -> None: ...
    async def demote(self, environment: str, names: Iterable[str]) -> None: ...
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
        conn.execute("PRAGMA busy_timeout = 5000")  # CLI + daemon share this file: wait, don't error
        conn.execute("PRAGMA cache_size = -65536")
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
                " physical_schema, physical_name, change_category, created_at, engine) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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

    async def get_snapshots(self, pairs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], Snapshot]:
        """Batch-fetch snapshots by (name, fingerprint) — two queries total, not 2N."""
        return await asyncio.to_thread(self._get_snapshots_sync, list(pairs))

    def _get_snapshots_sync(self, pairs: list[tuple[str, str]]) -> dict[tuple[str, str], Snapshot]:
        if not pairs:
            return {}
        if len(pairs) > 400:  # stay far under SQLITE_MAX_VARIABLE_NUMBER (32766 on conservative builds)
            merged: dict[tuple[str, str], Snapshot] = {}
            for start in range(0, len(pairs), 400):
                merged.update(self._get_snapshots_sync(pairs[start : start + 400]))
            return merged
        placeholders = ",".join(["(?,?)"] * len(pairs))
        flat = [value for pair in pairs for value in pair]
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM snapshots WHERE (name, fingerprint) IN (VALUES {placeholders})", flat
            ).fetchall()
            interval_rows = self._conn.execute(
                f"SELECT name, fingerprint, start_ts, end_ts FROM intervals "
                f"WHERE (name, fingerprint) IN (VALUES {placeholders})",
                flat,
            ).fetchall()
        intervals: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in interval_rows:
            intervals.setdefault((row["name"], row["fingerprint"]), []).append(row)
        return {
            (row["name"], row["fingerprint"]): _snapshot_from_row(
                row, _intervals_from_rows(intervals.get((row["name"], row["fingerprint"]), []))
            )
            for row in rows
        }

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

    async def list_snapshot_rows(self) -> list[dict[str, str]]:
        """Every snapshot row (no intervals): name, fingerprint, physical table, engine, created_at."""
        return await asyncio.to_thread(self._list_snapshot_rows_sync)

    def _list_snapshot_rows_sync(self) -> list[dict[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT name, fingerprint, physical_schema, physical_name, engine, created_at FROM snapshots"
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

    async def collect_snapshot_garbage(
        self, cutoff: datetime, *, delete: bool
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        """Partition snapshot rows into (doomed, surviving) and delete the doomed —
        one BEGIN IMMEDIATE transaction, so the reference check and the delete are
        atomic against a concurrent promote from any process. A row is doomed when
        no environment references its fingerprint AND it predates ``cutoff``.
        ``delete=False`` (dry run) returns the same partition without deleting."""
        return await asyncio.to_thread(self._collect_snapshot_garbage_sync, cutoff, delete)

    def _collect_snapshot_garbage_sync(
        self, cutoff: datetime, delete: bool
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        doomed: list[dict[str, str]] = []
        surviving: list[dict[str, str]] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                referenced = {
                    (row["model_name"], row["fingerprint"])
                    for row in self._conn.execute("SELECT DISTINCT model_name, fingerprint FROM environments")
                }
                rows = self._conn.execute(
                    "SELECT name, fingerprint, physical_schema, physical_name, engine, created_at FROM snapshots"
                ).fetchall()
                for row in map(dict, rows):
                    created = datetime.fromisoformat(row["created_at"])
                    if (row["name"], row["fingerprint"]) not in referenced and created < cutoff:
                        doomed.append(row)
                    else:
                        surviving.append(row)
                if delete and doomed:
                    pairs = [(row["name"], row["fingerprint"]) for row in doomed]
                    self._conn.executemany("DELETE FROM snapshots WHERE name = ? AND fingerprint = ?", pairs)
                    self._conn.executemany("DELETE FROM intervals WHERE name = ? AND fingerprint = ?", pairs)
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return doomed, surviving

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
        self._apply_promotion(environment, mapping, replace=False)

    def _latest_generation_mapping(self, environment: str) -> dict[str, str]:
        """The most recent promotion generation's full mapping (caller holds the lock)."""
        row = self._conn.execute(
            "SELECT coalesce(max(generation), 0) AS g FROM promotion_history WHERE environment = ?",
            (environment,),
        ).fetchone()
        if int(row["g"]) == 0:
            return {}
        return {
            r["model_name"]: r["fingerprint"]
            for r in self._conn.execute(
                "SELECT model_name, fingerprint FROM promotion_history WHERE environment = ? AND generation = ?",
                (environment, int(row["g"])),
            ).fetchall()
        }

    def _apply_promotion(self, environment: str, mapping: dict[str, str], *, replace: bool) -> None:
        """Move an environment's promotion pointers and record a history generation
        — all in ONE transaction. ``replace`` (rollback) also removes models not in
        ``mapping``; otherwise ``mapping`` is merged over the current pointers.

        A new generation is recorded ONLY when the resulting mapping differs from
        the latest one: a busy scheduler promoting the same fingerprints every run
        must not bury the real rollback target under identical generations, nor
        grow ``promotion_history`` without bound."""
        now = _now_iso()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                current = {
                    r["model_name"]: r["fingerprint"]
                    for r in self._conn.execute(
                        "SELECT model_name, fingerprint FROM environments WHERE environment = ?", (environment,)
                    ).fetchall()
                }
                if replace:
                    stale = [(environment, name) for name in current if name not in mapping]
                    self._conn.executemany("DELETE FROM environments WHERE environment = ? AND model_name = ?", stale)
                changed = [(environment, m, fp, now) for m, fp in mapping.items() if current.get(m) != fp]
                if changed:
                    self._conn.executemany(
                        "INSERT OR REPLACE INTO environments (environment, model_name, fingerprint, promoted_at) "
                        "VALUES (?, ?, ?, ?)",
                        changed,
                    )
                resulting = dict(mapping) if replace else {**current, **mapping}
                if resulting != self._latest_generation_mapping(environment):
                    row = self._conn.execute(
                        "SELECT coalesce(max(generation), 0) AS g FROM promotion_history WHERE environment = ?",
                        (environment,),
                    ).fetchone()
                    generation = int(row["g"]) + 1
                    self._conn.executemany(
                        "INSERT INTO promotion_history (environment, generation, model_name, fingerprint, promoted_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        [(environment, generation, m, fp, now) for m, fp in resulting.items()],
                    )
                self._conn.commit()
            except BaseException:  # never leave the shared connection inside an open txn
                self._conn.rollback()
                raise

    async def list_generations(self, environment: str) -> list[dict[str, object]]:
        """Promotion history, newest first: generation, when, how many models."""
        return await asyncio.to_thread(self._list_generations_sync, environment)

    def _list_generations_sync(self, environment: str) -> list[dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT generation, max(promoted_at) AS promoted_at, count(*) AS models "
                "FROM promotion_history WHERE environment = ? GROUP BY generation ORDER BY generation DESC",
                (environment,),
            ).fetchall()
        return [dict(row) for row in rows]

    async def get_generation(self, environment: str, generation: int) -> dict[str, str]:
        """The full model->fingerprint mapping recorded at ``generation``."""
        return await asyncio.to_thread(self._get_generation_sync, environment, generation)

    def _get_generation_sync(self, environment: str, generation: int) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT model_name, fingerprint FROM promotion_history WHERE environment = ? AND generation = ?",
                (environment, generation),
            ).fetchall()
        return {row["model_name"]: row["fingerprint"] for row in rows}

    async def set_environment(self, environment: str, mapping: dict[str, str]) -> None:
        """Replace an environment's mapping wholesale (rollback): rows not in
        ``mapping`` are removed. One transaction; records a new history generation."""
        await asyncio.to_thread(self._apply_promotion, environment, mapping, replace=True)

    async def trim_logs(
        self, older_than: timedelta = timedelta(days=30), *, keep_generations: int = 50
    ) -> dict[str, int]:
        """Trim ``event_log`` and ``check_results`` rows older than the cutoff, drop
        terminal ``work_queue`` rows of the same age, and keep only the most recent
        ``keep_generations`` promotion generations per environment. Each of these
        grows with every apply/flush/run and has no other reclamation path."""
        return await asyncio.to_thread(self._trim_logs_sync, older_than, keep_generations)

    def _trim_logs_sync(self, older_than: timedelta, keep_generations: int) -> dict[str, int]:
        cutoff = (datetime.now(UTC) - older_than).isoformat()
        with self._lock:
            events = self._conn.execute("DELETE FROM event_log WHERE ts < ?", (cutoff,)).rowcount
            checks = self._conn.execute("DELETE FROM check_results WHERE executed_at < ?", (cutoff,)).rowcount
            runs = self._conn.execute(
                "DELETE FROM work_queue WHERE enqueued_at < ? AND state IN ('succeeded', 'failed', 'cancelled')",
                (cutoff,),
            ).rowcount
            # keep the newest N generations per env; older rollback targets age out
            generations = self._conn.execute(
                "DELETE FROM promotion_history WHERE (environment, generation) IN ("
                "  SELECT environment, generation FROM ("
                "    SELECT environment, generation, "
                "           row_number() OVER (PARTITION BY environment ORDER BY generation DESC) AS rn "
                "    FROM (SELECT DISTINCT environment, generation FROM promotion_history)"
                "  ) WHERE rn > ?)",
                (keep_generations,),
            ).rowcount
            self._conn.commit()
        return {"events": events, "check_results": checks, "runs": runs, "generations": generations}

    async def demote(self, environment: str, names: Iterable[str]) -> None:
        """Remove models from an environment's promotion map (model deletion)."""
        await asyncio.to_thread(self._demote_sync, environment, list(names))

    def _demote_sync(self, environment: str, names: list[str]) -> None:
        if not names:
            return
        with self._lock:
            self._conn.executemany(
                "DELETE FROM environments WHERE environment = ? AND model_name = ?",
                [(environment, name) for name in names],
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

    async def delete_environment(self, environment: str) -> int:
        """Remove an environment's promotion rows; returns how many were deleted."""
        return await asyncio.to_thread(self._delete_environment_sync, environment)

    def _delete_environment_sync(self, environment: str) -> int:
        with self._lock:
            cursor = self._conn.execute("DELETE FROM environments WHERE environment = ?", (environment,))
            self._conn.commit()
        return int(cursor.rowcount)

    async def environment_promoted_at(self) -> dict[str, str]:
        """Each environment's most recent promotion timestamp."""
        return await asyncio.to_thread(self._environment_promoted_at_sync)

    def _environment_promoted_at_sync(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT environment, MAX(promoted_at) AS at FROM environments GROUP BY environment"
            ).fetchall()
        return {row["environment"]: row["at"] for row in rows}

    async def list_environments(self) -> list[str]:
        return await asyncio.to_thread(self._list_environments_sync)

    def _list_environments_sync(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute("SELECT DISTINCT environment FROM environments ORDER BY environment").fetchall()
        return [row["environment"] for row in rows]

    # --- work queue ---------------------------------------------------------

    async def enqueue_run(
        self,
        idempotency_key: str,
        flow_selector: list[str],
        partition: tuple[str | None, str | None] | None,
        priority: int = 0,
        *,
        restate: bool = False,
    ) -> bool:
        """Enqueue a run; returns False if an identical idempotency key is already queued.

        ``restate`` reprocesses every interval in the partition window instead of
        skipping the ones already filled (catchup)."""
        return await asyncio.to_thread(
            self._enqueue_run_sync, idempotency_key, flow_selector, partition, priority, restate
        )

    def _enqueue_run_sync(
        self,
        idempotency_key: str,
        flow_selector: list[str],
        partition: tuple[str | None, str | None] | None,
        priority: int,
        restate: bool,
    ) -> bool:
        with self._lock:
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO work_queue "
                "(idempotency_key, flow_selector, partition_start, partition_end, priority, enqueued_at, restate) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    idempotency_key or None,
                    json.dumps(flow_selector),
                    partition[0] if partition else None,
                    partition[1] if partition else None,
                    priority,
                    _now_iso(),
                    int(restate),
                ),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    async def claim_runs(
        self, limit: int = 10, *, owner: str = "worker", lease_seconds: float = 60.0, max_attempts: int = 3
    ) -> list[QueuedRun]:
        """Atomically claim queued runs — plus 'running' runs whose lease expired
        (their worker died). A reclaimed run past ``max_attempts`` is marked failed
        instead of being handed out again."""
        return await asyncio.to_thread(self._claim_runs_sync, limit, owner, lease_seconds, max_attempts)

    def _claim_runs_sync(self, limit: int, owner: str, lease_seconds: float, max_attempts: int) -> list[QueuedRun]:
        now = datetime.now(UTC)
        expires = (now + timedelta(seconds=lease_seconds)).isoformat()
        claimed: list[sqlite3.Row] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    "SELECT id, flow_selector, partition_start, partition_end, priority, attempts, restate, "
                    "       cancel_requested "
                    "FROM work_queue WHERE state = 'queued' "
                    "   OR (state = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?) "
                    "ORDER BY priority DESC, id LIMIT ?",
                    (now.isoformat(), limit),
                ).fetchall()
                for row in rows:
                    if row["cancel_requested"]:  # cancelled between attempts: honour it, don't re-run
                        self._conn.execute(
                            "UPDATE work_queue SET state = 'cancelled', lease_owner = NULL WHERE id = ?",
                            (row["id"],),
                        )
                        continue
                    if row["attempts"] >= max_attempts:  # a dead worker's run out of retries
                        self._conn.execute(
                            "UPDATE work_queue SET state = 'failed', error = ?, lease_owner = NULL WHERE id = ?",
                            (f"lease expired after {row['attempts']} attempt(s); retries exhausted", row["id"]),
                        )
                        continue
                    self._conn.execute(
                        "UPDATE work_queue SET state = 'running', attempts = attempts + 1, "
                        "lease_owner = ?, lease_expires_at = ?, cancel_requested = 0 WHERE id = ?",
                        (owner, expires, row["id"]),
                    )
                    claimed.append(row)
                self._conn.commit()
            except BaseException:  # never leave the shared connection inside an open txn
                self._conn.rollback()
                raise
        return [
            QueuedRun(
                id=row["id"],
                flow_selector=json.loads(row["flow_selector"]),
                partition_start=row["partition_start"],
                partition_end=row["partition_end"],
                priority=row["priority"],
                attempts=row["attempts"] + 1,
                restate=bool(row["restate"]),
            )
            for row in claimed
        ]

    async def renew_lease(self, run_id: int, *, owner: str, lease_seconds: float = 60.0) -> str:
        """Heartbeat: extend the lease. Returns "ok", "cancel" (cancellation was
        requested — stop cooperatively), or "lost" (another worker holds the run)."""
        return await asyncio.to_thread(self._renew_lease_sync, run_id, owner, lease_seconds)

    def _renew_lease_sync(self, run_id: int, owner: str, lease_seconds: float) -> str:
        # BEGIN IMMEDIATE + owner-fenced UPDATE: without it a starved worker whose
        # lease already expired can read its own stale ownership just before a
        # reclaimer's claim commits, then extend the reclaimer's lease — and both
        # workers execute the run. The fence is what makes reclaim safe.
        expires = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT lease_owner, cancel_requested FROM work_queue WHERE id = ? AND state = 'running'",
                    (run_id,),
                ).fetchone()
                if row is None or row["lease_owner"] != owner:
                    self._conn.commit()
                    return "lost"
                if row["cancel_requested"]:
                    self._conn.commit()
                    return "cancel"
                self._conn.execute(
                    "UPDATE work_queue SET lease_expires_at = ? WHERE id = ? AND lease_owner = ?",
                    (expires, run_id, owner),
                )
                self._conn.commit()
            except BaseException:  # never leave the shared connection inside an open txn
                self._conn.rollback()
                raise
        return "ok"

    async def request_cancel(self, run_id: int) -> str | None:
        """Cancel a run: queued runs cancel immediately; running runs get a
        cooperative flag their worker honours at the next heartbeat. Returns the
        resulting state, or None if the run is unknown/already finished."""
        return await asyncio.to_thread(self._request_cancel_sync, run_id)

    def _request_cancel_sync(self, run_id: int) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT state FROM work_queue WHERE id = ?", (run_id,)).fetchone()
            if row is None or row["state"] not in ("queued", "running"):
                return None
            if row["state"] == "queued":
                self._conn.execute("UPDATE work_queue SET state = 'cancelled' WHERE id = ?", (run_id,))
                self._conn.commit()
                return "cancelled"
            self._conn.execute("UPDATE work_queue SET cancel_requested = 1 WHERE id = ?", (run_id,))
            self._conn.commit()
        return "cancelling"

    async def requeue_run(self, run_id: int, *, error: str, owner: str | None = None) -> bool:
        """Put a failed attempt back on the queue for a durable retry (fenced like
        :meth:`finish_run` when ``owner`` is given). Returns whether it landed."""
        return await asyncio.to_thread(self._requeue_run_sync, run_id, error, owner)

    def _requeue_run_sync(self, run_id: int, error: str, owner: str | None) -> bool:
        fence = "" if owner is None else " AND lease_owner = ?"
        params: list[object] = [error, run_id]
        if owner is not None:
            params.append(owner)
        with self._lock:
            cursor = self._conn.execute(
                "UPDATE work_queue SET state = 'queued', error = ?, lease_owner = NULL, lease_expires_at = NULL "
                f"WHERE id = ?{fence}",
                params,
            )
            self._conn.commit()
        return cursor.rowcount > 0

    async def finish_run(
        self,
        run_id: int,
        *,
        success: bool,
        error: str | None = None,
        status: str | None = None,
        owner: str | None = None,
    ) -> bool:
        """Record a terminal state. With ``owner`` set, the write is fenced: it only
        lands while that worker still holds the lease — a starved worker whose run
        was reclaimed cannot stomp the reclaimer's result. Returns whether it landed."""
        return await asyncio.to_thread(self._finish_run_sync, run_id, success, error, status, owner)

    def _finish_run_sync(
        self, run_id: int, success: bool, error: str | None, status: str | None, owner: str | None
    ) -> bool:
        state = status or ("succeeded" if success else "failed")
        fence = "" if owner is None else " AND lease_owner = ?"
        params: list[object] = [state, error, run_id]
        if owner is not None:
            params.append(owner)
        with self._lock:
            cursor = self._conn.execute(
                "UPDATE work_queue SET state = ?, error = ?, lease_owner = NULL, lease_expires_at = NULL "
                f"WHERE id = ?{fence}",
                params,
            )
            self._conn.commit()
        return cursor.rowcount > 0

    async def count_pending_runs(self) -> int:
        return await asyncio.to_thread(self._count_pending_runs_sync)

    def _count_pending_runs_sync(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT count(*) FROM work_queue WHERE state IN ('queued', 'running')").fetchone()
        return int(row[0])

    _RUN_COLUMNS = (
        "id, idempotency_key, flow_selector, partition_start, partition_end, "
        "priority, state, attempts, error, enqueued_at, restate"
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
            restate=bool(row["restate"]),
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

    async def latest_event_seq(self) -> int:
        """The event log's current head (0 when empty) — where a live tail starts."""
        return await asyncio.to_thread(self._latest_event_seq_sync)

    def _latest_event_seq_sync(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT max(seq) FROM event_log").fetchone()
        return int(row[0] or 0)

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

    async def revoke_api_key(self, name: str) -> int:
        """Revoke every key with this name; returns how many were removed."""
        return await asyncio.to_thread(self._revoke_api_key_sync, name)

    def _revoke_api_key_sync(self, name: str) -> int:
        with self._lock:
            removed = self._conn.execute("DELETE FROM api_keys WHERE name = ?", (name,)).rowcount
            self._conn.commit()
        return removed

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
    """Apply pending migrations, each in its own transaction with the version bump
    inside it — a crash can never commit DDL without advancing user_version, and
    two processes opening a fresh database serialise on BEGIN IMMEDIATE (the loser
    re-reads the version and skips)."""
    while True:
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version >= len(_MIGRATIONS):
            return
        conn.execute("BEGIN IMMEDIATE")
        try:
            current = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if current != version:  # another process migrated while we waited
                conn.execute("ROLLBACK")
                continue
            for statement in _MIGRATIONS[version].split(";"):
                if statement.strip():
                    conn.execute(statement)
            conn.execute(f"PRAGMA user_version = {version + 1}")  # transactional in SQLite
            conn.commit()
        except Exception:
            conn.execute("ROLLBACK")
            raise
