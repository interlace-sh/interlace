"""The state store — the OLTP control-plane database.

Owns every non-warehouse table: snapshots, the interval ledger, environment
pointers and promotion history, the durable run queue, per-trigger state, the
event log, API keys, and check results. SQLite (WAL) is the single-node backend.
See docs/architecture/architecture.md §6 for why this is SQLite and not the
analytical DuckDB engine.

The tables share one connection (:class:`ControlDb`). Each surface is its own
type (``snapshots``, ``queue``, ``events``, ``keys``, ``checks``, ``triggers``,
``locks``, ``cdc``). :class:`SqliteStateStore` opens that connection and keeps
the historical methods, so plan/apply still depend only on :class:`StateStore`.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from interlace.state.advisory import AdvisoryLockStore
from interlace.state.cdc import CdcWatermarks
from interlace.state.checks import CheckStore
from interlace.state.codec import QueuedRun, RunRecord, event_actor
from interlace.state.conn import ControlDb
from interlace.state.events import EventLogStore
from interlace.state.interval import Interval, IntervalSet
from interlace.state.keys import ApiKeyStore
from interlace.state.queue import WorkQueue
from interlace.state.snapshot import Snapshot
from interlace.state.snapshots import SnapshotStore
from interlace.state.triggers import TriggerStore

__all__ = ["QueuedRun", "RunRecord", "SqliteStateStore", "StateStore", "event_actor"]


class StateStore(Protocol):
    """The plan/apply slice of the control-plane store — the surface differ and
    apply depend on. Daemon tables (queue, event log, API keys, locks, CDC)
    live on the composed stores, not on this protocol."""

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
    """SQLite-backed :class:`StateStore` (WAL mode).

    Open one of these per process. Plan and apply use the protocol methods.
    The daemon uses the attributes (``queue``, ``events``, ``keys``, ``cdc``, …)
    or the same-named methods on this object, which forward to them.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._db = ControlDb(connection)
        self.snapshots = SnapshotStore(self._db)
        self.queue = WorkQueue(self._db)
        self.events = EventLogStore(self._db)
        self.keys = ApiKeyStore(self._db)
        self.checks = CheckStore(self._db)
        self.triggers = TriggerStore(self._db)
        self.locks = AdvisoryLockStore(self._db)
        self.cdc = CdcWatermarks(self._db)

    @property
    def event_log_path(self) -> str | None:
        return self.events.path

    @event_log_path.setter
    def event_log_path(self, value: str | None) -> None:
        self.events.path = value

    @classmethod
    async def open(cls, path: str | Path) -> SqliteStateStore:
        connection = await asyncio.to_thread(ControlDb.connect, str(path))
        return cls(connection)

    async def close(self) -> None:
        await self._db.close()

    async def add_snapshot(self, snapshot: Snapshot) -> None:
        return await self.snapshots.add_snapshot(snapshot)

    async def get_snapshot(self, name: str, fingerprint: str) -> Snapshot | None:
        return await self.snapshots.get_snapshot(name, fingerprint)

    async def get_snapshots(self, pairs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], Snapshot]:
        """Batch-fetch snapshots by (name, fingerprint) — two queries total, not 2N."""
        return await self.snapshots.get_snapshots(pairs)

    async def list_snapshots(self, name: str) -> list[Snapshot]:
        return await self.snapshots.list_snapshots(name)

    async def list_snapshot_rows(self) -> list[dict[str, str]]:
        """Every snapshot row (no intervals): name, fingerprint, physical table, engine, created_at."""
        return await self.snapshots.list_snapshot_rows()

    async def delete_snapshots(self, pairs: Iterable[tuple[str, str]]) -> None:
        """Remove snapshot rows and their interval-ledger entries."""
        return await self.snapshots.delete_snapshots(pairs)

    async def collect_snapshot_garbage(
        self, cutoff: datetime, *, delete: bool
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        """Partition snapshot rows into (doomed, surviving) and delete the doomed —
        one BEGIN IMMEDIATE transaction, so the reference check and the delete are
        atomic against a concurrent promote from any process. A row is doomed when
        no environment references its fingerprint AND it predates ``cutoff``.
        ``delete=False`` (dry run) returns the same partition without deleting."""
        return await self.snapshots.collect_snapshot_garbage(cutoff, delete=delete)

    async def record_interval(self, name: str, fingerprint: str, interval: Interval) -> None:
        return await self.snapshots.record_interval(name, fingerprint, interval)

    async def get_intervals(self, name: str, fingerprint: str) -> IntervalSet:
        return await self.snapshots.get_intervals(name, fingerprint)

    async def promote(self, environment: str, mapping: dict[str, str]) -> None:
        return await self.snapshots.promote(environment, mapping)

    async def list_generations(self, environment: str) -> list[dict[str, object]]:
        """Promotion history, newest first: generation, when, how many models."""
        return await self.snapshots.list_generations(environment)

    async def get_generation(self, environment: str, generation: int) -> dict[str, str]:
        """The full model->fingerprint mapping recorded at ``generation``."""
        return await self.snapshots.get_generation(environment, generation)

    async def set_environment(self, environment: str, mapping: dict[str, str]) -> None:
        """Replace an environment's mapping wholesale (rollback): rows not in
        ``mapping`` are removed. One transaction; records a new history generation."""
        return await self.snapshots.set_environment(environment, mapping)

    async def demote(self, environment: str, names: Iterable[str]) -> None:
        """Remove models from an environment's promotion map (model deletion)."""
        return await self.snapshots.demote(environment, names)

    async def get_environment(self, environment: str) -> dict[str, str]:
        return await self.snapshots.get_environment(environment)

    async def delete_environment(self, environment: str) -> int:
        """Remove an environment's promotion rows; returns how many were deleted."""
        return await self.snapshots.delete_environment(environment)

    async def environment_promoted_at(self) -> dict[str, str]:
        """Each environment's most recent promotion timestamp."""
        return await self.snapshots.environment_promoted_at()

    async def list_environments(self) -> list[str]:
        return await self.snapshots.list_environments()

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
        return await self.queue.enqueue_run(idempotency_key, flow_selector, partition, priority, restate=restate)

    async def claim_runs(
        self, limit: int = 10, *, owner: str = "worker", lease_seconds: float = 60.0, max_attempts: int = 3
    ) -> list[QueuedRun]:
        """Atomically claim queued runs — plus 'running' runs whose lease expired
        (their worker died). A reclaimed run past ``max_attempts`` is marked failed
        instead of being handed out again."""
        return await self.queue.claim_runs(limit, owner=owner, lease_seconds=lease_seconds, max_attempts=max_attempts)

    async def renew_lease(self, run_id: int, *, owner: str, lease_seconds: float = 60.0) -> str:
        """Heartbeat: extend the lease. Returns "ok", "cancel" (cancellation was
        requested — stop cooperatively), or "lost" (another worker holds the run)."""
        return await self.queue.renew_lease(run_id, owner=owner, lease_seconds=lease_seconds)

    def renew_lease_sync(self, run_id: int, *, owner: str, lease_seconds: float = 60.0) -> str:
        """Same verdicts as :meth:`renew_lease`, called off the event loop.

        The run heartbeat uses this so a model that blocks the loop still keeps
        its lease. The lease is how soon a dead process is reclaimed, not how
        long a model may run.
        """
        return self.queue.renew_lease_sync(run_id, owner, lease_seconds)

    async def request_cancel(self, run_id: int) -> str | None:
        """Cancel a run: queued runs cancel immediately; running runs get a
        cooperative flag their worker honours at the next heartbeat. Returns the
        resulting state, or None if the run is unknown/already finished."""
        return await self.queue.request_cancel(run_id)

    async def requeue_run(self, run_id: int, *, error: str, owner: str | None = None) -> bool:
        """Put a failed attempt back on the queue for a durable retry (fenced like
        :meth:`finish_run` when ``owner`` is given). Returns whether it landed."""
        return await self.queue.requeue_run(run_id, error=error, owner=owner)

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
        return await self.queue.finish_run(run_id, success=success, error=error, status=status, owner=owner)

    async def count_pending_runs(self) -> int:
        return await self.queue.count_pending_runs()

    async def list_runs(self, limit: int = 50) -> list[RunRecord]:
        return await self.queue.list_runs(limit)

    async def get_run(self, run_id: int) -> RunRecord | None:
        return await self.queue.get_run(run_id)

    async def events_for_entity(self, entity: str) -> list[dict[str, object]]:
        return await self.events.events_for_entity(entity)

    async def latest_model_build(self, model: str) -> dict[str, object] | None:
        """The newest terminal build event for a model (done / failed / cancelled)."""
        return await self.events.latest_model_build(model)

    async def events_for_run(self, run_id: int) -> list[dict[str, object]]:
        """A run's per-model events — keyed by ``payload.run`` (their entity is the model
        name, not the run id), so the run detail can show a model-level timeline."""
        return await self.events.events_for_run(run_id)

    async def append_event(self, type: str, entity: str | None = None, payload: dict[str, object] | None = None) -> int:
        return await self.events.append_event(type, entity, payload)

    async def latest_event_seq(self) -> int:
        """The event log's current head (0 when empty) — where a live tail starts."""
        return await self.events.latest_event_seq()

    async def read_events(self, after_seq: int = 0, limit: int = 200) -> list[dict[str, object]]:
        return await self.events.read_events(after_seq, limit)

    async def create_api_key(self, name: str, scopes: list[str]) -> str:
        """Create a key; returns the plaintext token (shown once — only the hash is stored)."""
        return await self.keys.create_api_key(name, scopes)

    async def verify_api_key(self, token: str) -> tuple[str, list[str]] | None:
        """Return ``(name, scopes)``, or None if the token is unknown."""
        return await self.keys.verify_api_key(token)

    async def revoke_api_key(self, name: str) -> int:
        """Revoke every key with this name; returns how many were removed."""
        return await self.keys.revoke_api_key(name)

    async def count_api_keys(self) -> int:
        return await self.keys.count_api_keys()

    async def list_api_keys(self) -> list[dict[str, object]]:
        return await self.keys.list_api_keys()

    async def record_check_results(self, environment: str, fingerprint: str, outcomes: Iterable[Any]) -> None:
        """Persist one model's check outcomes (objects with name/type/severity/status/failures/message)."""
        return await self.checks.record_check_results(environment, fingerprint, outcomes)

    async def list_check_results(self, model: str | None = None, limit: int = 200) -> list[dict[str, object]]:
        return await self.checks.list_check_results(model, limit)

    async def get_trigger_last_fired(self, trigger_id: str) -> datetime | None:
        return await self.triggers.get_trigger_last_fired(trigger_id)

    async def set_trigger_last_fired(self, trigger_id: str, when: datetime) -> None:
        return await self.triggers.set_trigger_last_fired(trigger_id, when)

    async def acquire_lock(self, name: str, *, owner: str, lease_seconds: float = 180.0, timeout: float = 60.0) -> bool:
        """Take (or renew, if already owned) a named advisory lock. Returns False on timeout."""
        return await self.locks.acquire_lock(name, owner=owner, lease_seconds=lease_seconds, timeout=timeout)

    async def renew_lock(self, name: str, *, owner: str, lease_seconds: float = 180.0) -> bool:
        """Extend a lock only if ``owner`` still holds it. Returns False if lost."""
        return await self.locks.renew_lock(name, owner=owner, lease_seconds=lease_seconds)

    async def release_lock(self, name: str, *, owner: str) -> None:
        """Drop a lock if ``owner`` still holds it (no-op otherwise)."""
        return await self.locks.release_lock(name, owner=owner)

    async def cdc_confirmed_lsn(self, stream: str) -> str | None:
        """The replication LSN whose rows have been flushed, or None if CDC has not confirmed one."""
        return await self.cdc.cdc_confirmed_lsn(stream)

    async def cdc_note_pending(self, stream: str, rows: list[tuple[int, str]]) -> None:
        """Remember which log offset each replication LSN landed at. Deduped appends are included."""
        return await self.cdc.cdc_note_pending(stream, rows)

    async def cdc_advance(self, stream: str, watermark: int) -> str | None:
        """Confirm the LSN of every pending row at or below ``watermark``.

        ``watermark`` is the offset ``flush_streams`` has committed. Rows past it
        stay pending, so a crash re-reads from the last confirmed LSN."""
        return await self.cdc.cdc_advance(stream, watermark)

    async def trim_logs(
        self, older_than: timedelta = timedelta(days=30), *, keep_generations: int = 50
    ) -> dict[str, int]:
        """Trim ``event_log`` and ``check_results`` rows older than the cutoff, drop
        terminal ``work_queue`` rows of the same age, and keep only the most recent
        ``keep_generations`` promotion generations per environment. Each of these
        grows with every apply/flush/run and has no other reclamation path."""
        return await self._db.io(self._trim_logs_sync, older_than, keep_generations)

    def _trim_logs_sync(self, older_than: timedelta, keep_generations: int) -> dict[str, int]:
        cutoff = (datetime.now(UTC) - older_than).isoformat()
        with self._db.lock:
            events = self._db.conn.execute("DELETE FROM event_log WHERE ts < ?", (cutoff,)).rowcount
            checks = self._db.conn.execute("DELETE FROM check_results WHERE executed_at < ?", (cutoff,)).rowcount
            runs = self._db.conn.execute(
                "DELETE FROM work_queue WHERE enqueued_at < ? AND state IN ('succeeded', 'failed', 'cancelled')",
                (cutoff,),
            ).rowcount
            # keep the newest N generations per env; older rollback targets age out
            generations = self._db.conn.execute(
                "DELETE FROM promotion_history WHERE (environment, generation) IN ("
                "  SELECT environment, generation FROM ("
                "    SELECT environment, generation, "
                "           row_number() OVER (PARTITION BY environment ORDER BY generation DESC) AS rn "
                "    FROM (SELECT DISTINCT environment, generation FROM promotion_history)"
                "  ) WHERE rn > ?)",
                (keep_generations,),
            ).rowcount
            self._db.conn.commit()
        return {"events": events, "check_results": checks, "runs": runs, "generations": generations}

    async def reset_control_plane(self, keep_models: Iterable[str] = ()) -> dict[str, int]:
        """Wipe operational control-plane rows, keeping API keys, advisory locks,
        trigger last-fired times, and snapshot/interval/environment rows for
        ``keep_models`` (terminal table/file models — so the next apply does not
        re-deliver into destinations we do not own)."""
        return await self._db.io(self._reset_control_plane_sync, list(keep_models))

    def _reset_control_plane_sync(self, keep_models: list[str]) -> dict[str, int]:
        # snapshots / intervals / environments are filtered; the rest go entirely.
        # api_keys and advisory_locks (the apply lock we hold) and trigger_state
        # (so a live scheduler does not immediately force-run terminals) stay.
        wipe_all = ("work_queue", "event_log", "check_results", "promotion_history")
        counts: dict[str, int] = {}
        with self._db.lock:
            self._db.conn.execute("BEGIN IMMEDIATE")
            try:
                if keep_models:
                    placeholders = ",".join("?" * len(keep_models))
                    for table in ("snapshots", "intervals"):
                        cursor = self._db.conn.execute(
                            f"DELETE FROM {table} WHERE name NOT IN ({placeholders})",  # noqa: S608
                            keep_models,
                        )
                        counts[table] = int(cursor.rowcount)
                    cursor = self._db.conn.execute(
                        f"DELETE FROM environments WHERE model_name NOT IN ({placeholders})",  # noqa: S608
                        keep_models,
                    )
                    counts["environments"] = int(cursor.rowcount)
                else:
                    for table in ("snapshots", "intervals", "environments"):
                        cursor = self._db.conn.execute(f"DELETE FROM {table}")  # noqa: S608 — fixed names
                        counts[table] = int(cursor.rowcount)
                for table in wipe_all:
                    cursor = self._db.conn.execute(f"DELETE FROM {table}")  # noqa: S608 — fixed names
                    counts[table] = int(cursor.rowcount)
                self._db.conn.commit()
            except BaseException:
                self._db.conn.rollback()
                raise
        return counts
