"""Durable run queue: enqueue, lease, cancel, and finish."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta

from interlace.state.codec import QueuedRun, RunRecord, _now_iso
from interlace.state.conn import ControlDb


class WorkQueue:
    """The durable work queue on one control-plane connection."""

    def __init__(self, db: ControlDb) -> None:
        self._db = db

    _RUN_COLUMNS = (
        "id, idempotency_key, flow_selector, partition_start, partition_end, "
        "priority, state, attempts, error, enqueued_at, restate"
    )

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
        return await self._db.io(self._enqueue_run_sync, idempotency_key, flow_selector, partition, priority, restate)

    def _enqueue_run_sync(
        self,
        idempotency_key: str,
        flow_selector: list[str],
        partition: tuple[str | None, str | None] | None,
        priority: int,
        restate: bool,
    ) -> bool:
        with self._db.lock:
            cursor = self._db.conn.execute(
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
            self._db.conn.commit()
            return cursor.rowcount > 0

    async def claim_runs(
        self, limit: int = 10, *, owner: str = "worker", lease_seconds: float = 60.0, max_attempts: int = 3
    ) -> list[QueuedRun]:
        """Atomically claim queued runs — plus 'running' runs whose lease expired
        (their worker died). A reclaimed run past ``max_attempts`` is marked failed
        instead of being handed out again."""
        return await self._db.io(self._claim_runs_sync, limit, owner, lease_seconds, max_attempts)

    def _claim_runs_sync(self, limit: int, owner: str, lease_seconds: float, max_attempts: int) -> list[QueuedRun]:
        now = datetime.now(UTC)
        expires = (now + timedelta(seconds=lease_seconds)).isoformat()
        claimed: list[sqlite3.Row] = []
        with self._db.lock:
            self._db.conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._db.conn.execute(
                    "SELECT id, flow_selector, partition_start, partition_end, priority, attempts, restate, "
                    "       cancel_requested "
                    "FROM work_queue WHERE state = 'queued' "
                    "   OR (state = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?) "
                    "ORDER BY priority DESC, id LIMIT ?",
                    (now.isoformat(), limit),
                ).fetchall()
                for row in rows:
                    if row["cancel_requested"]:  # cancelled between attempts: honour it, don't re-run
                        self._db.conn.execute(
                            "UPDATE work_queue SET state = 'cancelled', lease_owner = NULL WHERE id = ?",
                            (row["id"],),
                        )
                        continue
                    if row["attempts"] >= max_attempts:  # a dead worker's run out of retries
                        self._db.conn.execute(
                            "UPDATE work_queue SET state = 'failed', error = ?, lease_owner = NULL WHERE id = ?",
                            (f"lease expired after {row['attempts']} attempt(s); retries exhausted", row["id"]),
                        )
                        continue
                    self._db.conn.execute(
                        "UPDATE work_queue SET state = 'running', attempts = attempts + 1, "
                        "lease_owner = ?, lease_expires_at = ?, cancel_requested = 0 WHERE id = ?",
                        (owner, expires, row["id"]),
                    )
                    claimed.append(row)
                self._db.conn.commit()
            except BaseException:  # never leave the shared connection inside an open txn
                self._db.conn.rollback()
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
        return await self._db.io(self._renew_lease_sync, run_id, owner, lease_seconds)

    def _renew_lease_sync(self, run_id: int, owner: str, lease_seconds: float) -> str:
        # BEGIN IMMEDIATE + owner-fenced UPDATE: without it a starved worker whose
        # lease already expired can read its own stale ownership just before a
        # reclaimer's claim commits, then extend the reclaimer's lease — and both
        # workers execute the run. The fence is what makes reclaim safe.
        expires = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()
        with self._db.lock:
            self._db.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.conn.execute(
                    "SELECT lease_owner, cancel_requested FROM work_queue WHERE id = ? AND state = 'running'",
                    (run_id,),
                ).fetchone()
                if row is None or row["lease_owner"] != owner:
                    self._db.conn.commit()
                    return "lost"
                if row["cancel_requested"]:
                    self._db.conn.commit()
                    return "cancel"
                self._db.conn.execute(
                    "UPDATE work_queue SET lease_expires_at = ? WHERE id = ? AND lease_owner = ?",
                    (expires, run_id, owner),
                )
                self._db.conn.commit()
            except BaseException:  # never leave the shared connection inside an open txn
                self._db.conn.rollback()
                raise
        return "ok"

    async def request_cancel(self, run_id: int) -> str | None:
        """Cancel a run: queued runs cancel immediately; running runs get a
        cooperative flag their worker honours at the next heartbeat. Returns the
        resulting state, or None if the run is unknown/already finished."""
        return await self._db.io(self._request_cancel_sync, run_id)

    def _request_cancel_sync(self, run_id: int) -> str | None:
        with self._db.lock:
            row = self._db.conn.execute("SELECT state FROM work_queue WHERE id = ?", (run_id,)).fetchone()
            if row is None or row["state"] not in ("queued", "running"):
                return None
            if row["state"] == "queued":
                self._db.conn.execute("UPDATE work_queue SET state = 'cancelled' WHERE id = ?", (run_id,))
                self._db.conn.commit()
                return "cancelled"
            self._db.conn.execute("UPDATE work_queue SET cancel_requested = 1 WHERE id = ?", (run_id,))
            self._db.conn.commit()
        return "cancelling"

    async def requeue_run(self, run_id: int, *, error: str, owner: str | None = None) -> bool:
        """Put a failed attempt back on the queue for a durable retry (fenced like
        :meth:`finish_run` when ``owner`` is given). Returns whether it landed."""
        return await self._db.io(self._requeue_run_sync, run_id, error, owner)

    def _requeue_run_sync(self, run_id: int, error: str, owner: str | None) -> bool:
        fence = "" if owner is None else " AND lease_owner = ?"
        params: list[object] = [error, run_id]
        if owner is not None:
            params.append(owner)
        with self._db.lock:
            cursor = self._db.conn.execute(
                "UPDATE work_queue SET state = 'queued', error = ?, lease_owner = NULL, lease_expires_at = NULL "
                f"WHERE id = ?{fence}",
                params,
            )
            self._db.conn.commit()
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
        return await self._db.io(self._finish_run_sync, run_id, success, error, status, owner)

    def _finish_run_sync(
        self, run_id: int, success: bool, error: str | None, status: str | None, owner: str | None
    ) -> bool:
        state = status or ("succeeded" if success else "failed")
        fence = "" if owner is None else " AND lease_owner = ?"
        params: list[object] = [state, error, run_id]
        if owner is not None:
            params.append(owner)
        with self._db.lock:
            cursor = self._db.conn.execute(
                "UPDATE work_queue SET state = ?, error = ?, lease_owner = NULL, lease_expires_at = NULL "
                f"WHERE id = ?{fence}",
                params,
            )
            self._db.conn.commit()
        return cursor.rowcount > 0

    async def count_pending_runs(self) -> int:
        return await self._db.io(self._count_pending_runs_sync)

    def _count_pending_runs_sync(self) -> int:
        with self._db.lock:
            row = self._db.conn.execute(
                "SELECT count(*) FROM work_queue WHERE state IN ('queued', 'running')"
            ).fetchone()
        return int(row[0])

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
        return await self._db.io(self._list_runs_sync, limit)

    def _list_runs_sync(self, limit: int) -> list[RunRecord]:
        # correlated subqueries on idx_event_log_entity give each run its wall-clock
        # span (run.started → terminal) and the env it built into, without a per-run
        # round-trip — so the list can show duration + env, not just enqueue time
        with self._db.lock:
            rows = self._db.conn.execute(
                f"SELECT {self._RUN_COLUMNS}, "
                "  (SELECT ts FROM event_log e WHERE e.entity = CAST(work_queue.id AS TEXT) "
                "     AND e.type = 'run.started' ORDER BY e.seq LIMIT 1) AS started_at, "
                "  (SELECT ts FROM event_log e WHERE e.entity = CAST(work_queue.id AS TEXT) "
                "     AND e.type IN ('run.succeeded', 'run.failed', 'run.cancelled') "
                "     ORDER BY e.seq DESC LIMIT 1) AS finished_at, "
                "  (SELECT json_extract(e.payload, '$.environment') FROM event_log e "
                "     WHERE e.entity = CAST(work_queue.id AS TEXT) AND e.type = 'run.succeeded' "
                "     ORDER BY e.seq DESC LIMIT 1) AS environment "
                "FROM work_queue ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        records = []
        for row in rows:
            record = self._run_dict(row)
            record["started_at"] = row["started_at"]
            record["finished_at"] = row["finished_at"]
            record["environment"] = row["environment"]
            records.append(record)
        return records

    async def get_run(self, run_id: int) -> RunRecord | None:
        return await self._db.io(self._get_run_sync, run_id)

    def _get_run_sync(self, run_id: int) -> RunRecord | None:
        with self._db.lock:
            row = self._db.conn.execute(
                f"SELECT {self._RUN_COLUMNS} FROM work_queue WHERE id = ?", (run_id,)
            ).fetchone()
        return self._run_dict(row) if row else None
