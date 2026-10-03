"""Snapshots, the interval ledger, and environment promotion pointers."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import datetime

from interlace.state.codec import (
    _intervals_from_rows,
    _now_iso,
    _snapshot_from_row,
    _snapshot_to_row,
)
from interlace.state.conn import ControlDb
from interlace.state.interval import Interval, IntervalSet
from interlace.state.snapshot import Snapshot


class SnapshotStore:
    """Snapshots, intervals, and environment pointers on one control-plane connection."""

    def __init__(self, db: ControlDb) -> None:
        self._db = db

    async def add_snapshot(self, snapshot: Snapshot) -> None:
        await self._db.io(self._add_snapshot_sync, snapshot)

    def _add_snapshot_sync(self, snapshot: Snapshot) -> None:
        with self._db.lock:
            self._db.conn.execute(
                "INSERT OR REPLACE INTO snapshots "
                "(name, fingerprint, local_fingerprint, metadata_hash, definition_sql, physical_catalog, "
                " physical_schema, physical_name, change_category, created_at, engine, physical_hash, "
                " physical_objects) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                _snapshot_to_row(snapshot),
            )
            self._db.conn.execute(
                "DELETE FROM intervals WHERE name = ? AND fingerprint = ?",
                (snapshot.name, snapshot.fingerprint),
            )
            self._db.conn.executemany(
                "INSERT INTO intervals (name, fingerprint, start_ts, end_ts) VALUES (?, ?, ?, ?)",
                [
                    (snapshot.name, snapshot.fingerprint, iv.start.isoformat(), iv.end.isoformat())
                    for iv in snapshot.intervals
                ],
            )
            self._db.conn.commit()

    async def get_snapshot(self, name: str, fingerprint: str) -> Snapshot | None:
        return await self._db.io(self._get_snapshot_sync, name, fingerprint)

    def _get_snapshot_sync(self, name: str, fingerprint: str) -> Snapshot | None:
        with self._db.lock:
            row = self._db.conn.execute(
                "SELECT * FROM snapshots WHERE name = ? AND fingerprint = ?", (name, fingerprint)
            ).fetchone()
            if row is None:
                return None
            interval_rows = self._db.conn.execute(
                "SELECT start_ts, end_ts FROM intervals WHERE name = ? AND fingerprint = ?", (name, fingerprint)
            ).fetchall()
        return _snapshot_from_row(row, _intervals_from_rows(interval_rows))

    async def get_snapshots(self, pairs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], Snapshot]:
        """Batch-fetch snapshots by (name, fingerprint) — two queries total, not 2N."""
        return await self._db.io(self._get_snapshots_sync, list(pairs))

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
        with self._db.lock:
            rows = self._db.conn.execute(
                f"SELECT * FROM snapshots WHERE (name, fingerprint) IN (VALUES {placeholders})", flat
            ).fetchall()
            interval_rows = self._db.conn.execute(
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
        return await self._db.io(self._list_snapshots_sync, name)

    def _list_snapshots_sync(self, name: str) -> list[Snapshot]:
        with self._db.lock:
            rows = self._db.conn.execute(
                "SELECT * FROM snapshots WHERE name = ? ORDER BY created_at", (name,)
            ).fetchall()
            result = []
            for row in rows:
                interval_rows = self._db.conn.execute(
                    "SELECT start_ts, end_ts FROM intervals WHERE name = ? AND fingerprint = ?",
                    (name, row["fingerprint"]),
                ).fetchall()
                result.append(_snapshot_from_row(row, _intervals_from_rows(interval_rows)))
        return result

    async def list_snapshot_rows(self) -> list[dict[str, str]]:
        """Every snapshot row (no intervals): name, fingerprint, physical table, engine, created_at."""
        return await self._db.io(self._list_snapshot_rows_sync)

    def _list_snapshot_rows_sync(self) -> list[dict[str, str]]:
        with self._db.lock:
            rows = self._db.conn.execute(
                "SELECT name, fingerprint, physical_schema, physical_name, engine, created_at FROM snapshots"
            ).fetchall()
        return [dict(row) for row in rows]

    async def delete_snapshots(self, pairs: Iterable[tuple[str, str]]) -> None:
        """Remove snapshot rows and their interval-ledger entries."""
        await self._db.io(self._delete_snapshots_sync, list(pairs))

    def _delete_snapshots_sync(self, pairs: list[tuple[str, str]]) -> None:
        with self._db.lock:
            self._db.conn.executemany("DELETE FROM snapshots WHERE name = ? AND fingerprint = ?", pairs)
            self._db.conn.executemany("DELETE FROM intervals WHERE name = ? AND fingerprint = ?", pairs)
            self._db.conn.commit()

    async def collect_snapshot_garbage(
        self, cutoff: datetime, *, delete: bool
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        """Partition snapshot rows into (doomed, surviving) and delete the doomed —
        one BEGIN IMMEDIATE transaction, so the reference check and the delete are
        atomic against a concurrent promote from any process. A row is doomed when
        no environment references its fingerprint AND it predates ``cutoff``.
        ``delete=False`` (dry run) returns the same partition without deleting."""
        return await self._db.io(self._collect_snapshot_garbage_sync, cutoff, delete)

    def _collect_snapshot_garbage_sync(
        self, cutoff: datetime, delete: bool
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        doomed: list[dict[str, str]] = []
        surviving: list[dict[str, str]] = []
        with self._db.lock:
            self._db.conn.execute("BEGIN IMMEDIATE")
            try:
                referenced = {
                    (row["model_name"], row["fingerprint"])
                    for row in self._db.conn.execute(
                        "SELECT DISTINCT model_name, fingerprint FROM environments"
                    ).fetchall()
                }
                rows = self._db.conn.execute(
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
                    self._db.conn.executemany("DELETE FROM snapshots WHERE name = ? AND fingerprint = ?", pairs)
                    self._db.conn.executemany("DELETE FROM intervals WHERE name = ? AND fingerprint = ?", pairs)
                self._db.conn.commit()
            except BaseException:
                self._db.conn.rollback()
                raise
        return doomed, surviving

    async def record_interval(self, name: str, fingerprint: str, interval: Interval) -> None:
        await self._db.io(self._record_interval_sync, name, fingerprint, interval)

    def _record_interval_sync(self, name: str, fingerprint: str, interval: Interval) -> None:
        with self._db.lock:
            self._db.conn.execute(
                "INSERT OR IGNORE INTO intervals (name, fingerprint, start_ts, end_ts) VALUES (?, ?, ?, ?)",
                (name, fingerprint, interval.start.isoformat(), interval.end.isoformat()),
            )
            self._db.conn.commit()

    async def get_intervals(self, name: str, fingerprint: str) -> IntervalSet:
        return await self._db.io(self._get_intervals_sync, name, fingerprint)

    def _get_intervals_sync(self, name: str, fingerprint: str) -> IntervalSet:
        with self._db.lock:
            rows = self._db.conn.execute(
                "SELECT start_ts, end_ts FROM intervals WHERE name = ? AND fingerprint = ?", (name, fingerprint)
            ).fetchall()
        return _intervals_from_rows(rows)

    async def promote(self, environment: str, mapping: dict[str, str]) -> None:
        await self._db.io(self._promote_sync, environment, mapping)

    def _promote_sync(self, environment: str, mapping: dict[str, str]) -> None:
        self._apply_promotion(environment, mapping, replace=False)

    def _latest_generation_mapping(self, environment: str) -> dict[str, str]:
        """The most recent promotion generation's full mapping (caller holds the lock)."""
        row = self._db.conn.execute(
            "SELECT coalesce(max(generation), 0) AS g FROM promotion_history WHERE environment = ?",
            (environment,),
        ).fetchone()
        if int(row["g"]) == 0:
            return {}
        return {
            r["model_name"]: r["fingerprint"]
            for r in self._db.conn.execute(
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
        with self._db.lock:
            self._db.conn.execute("BEGIN IMMEDIATE")
            try:
                current = {
                    r["model_name"]: r["fingerprint"]
                    for r in self._db.conn.execute(
                        "SELECT model_name, fingerprint FROM environments WHERE environment = ?", (environment,)
                    ).fetchall()
                }
                if replace:
                    stale = [(environment, name) for name in current if name not in mapping]
                    self._db.conn.executemany(
                        "DELETE FROM environments WHERE environment = ? AND model_name = ?", stale
                    )
                changed = [(environment, m, fp, now) for m, fp in mapping.items() if current.get(m) != fp]
                if changed:
                    self._db.conn.executemany(
                        "INSERT OR REPLACE INTO environments (environment, model_name, fingerprint, promoted_at) "
                        "VALUES (?, ?, ?, ?)",
                        changed,
                    )
                resulting = dict(mapping) if replace else {**current, **mapping}
                if resulting != self._latest_generation_mapping(environment):
                    row = self._db.conn.execute(
                        "SELECT coalesce(max(generation), 0) AS g FROM promotion_history WHERE environment = ?",
                        (environment,),
                    ).fetchone()
                    generation = int(row["g"]) + 1
                    self._db.conn.executemany(
                        "INSERT INTO promotion_history (environment, generation, model_name, fingerprint, promoted_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        [(environment, generation, m, fp, now) for m, fp in resulting.items()],
                    )
                self._db.conn.commit()
            except BaseException:  # never leave the shared connection inside an open txn
                self._db.conn.rollback()
                raise

    async def list_generations(self, environment: str) -> list[dict[str, object]]:
        """Promotion history, newest first: generation, when, how many models."""
        return await self._db.io(self._list_generations_sync, environment)

    def _list_generations_sync(self, environment: str) -> list[dict[str, object]]:
        with self._db.lock:
            rows = self._db.conn.execute(
                "SELECT generation, max(promoted_at) AS promoted_at, count(*) AS models "
                "FROM promotion_history WHERE environment = ? GROUP BY generation ORDER BY generation DESC",
                (environment,),
            ).fetchall()
        return [dict(row) for row in rows]

    async def get_generation(self, environment: str, generation: int) -> dict[str, str]:
        """The full model->fingerprint mapping recorded at ``generation``."""
        return await self._db.io(self._get_generation_sync, environment, generation)

    def _get_generation_sync(self, environment: str, generation: int) -> dict[str, str]:
        with self._db.lock:
            rows = self._db.conn.execute(
                "SELECT model_name, fingerprint FROM promotion_history WHERE environment = ? AND generation = ?",
                (environment, generation),
            ).fetchall()
        return {row["model_name"]: row["fingerprint"] for row in rows}

    async def set_environment(self, environment: str, mapping: dict[str, str]) -> None:
        """Replace an environment's mapping wholesale (rollback): rows not in
        ``mapping`` are removed. One transaction; records a new history generation."""
        await self._db.io(self._apply_promotion, environment, mapping, replace=True)

    async def demote(self, environment: str, names: Iterable[str]) -> None:
        """Remove models from an environment's promotion map (model deletion)."""
        await self._db.io(self._demote_sync, environment, list(names))

    def _demote_sync(self, environment: str, names: list[str]) -> None:
        if not names:
            return
        with self._db.lock:
            self._db.conn.executemany(
                "DELETE FROM environments WHERE environment = ? AND model_name = ?",
                [(environment, name) for name in names],
            )
            self._db.conn.commit()

    async def get_environment(self, environment: str) -> dict[str, str]:
        return await self._db.io(self._get_environment_sync, environment)

    def _get_environment_sync(self, environment: str) -> dict[str, str]:
        with self._db.lock:
            rows = self._db.conn.execute(
                "SELECT model_name, fingerprint FROM environments WHERE environment = ?", (environment,)
            ).fetchall()
        return {row["model_name"]: row["fingerprint"] for row in rows}

    async def delete_environment(self, environment: str) -> int:
        """Remove an environment's promotion rows; returns how many were deleted."""
        return await self._db.io(self._delete_environment_sync, environment)

    def _delete_environment_sync(self, environment: str) -> int:
        with self._db.lock:
            cursor = self._db.conn.execute("DELETE FROM environments WHERE environment = ?", (environment,))
            self._db.conn.commit()
        return int(cursor.rowcount)

    async def environment_promoted_at(self) -> dict[str, str]:
        """Each environment's most recent promotion timestamp."""
        return await self._db.io(self._environment_promoted_at_sync)

    def _environment_promoted_at_sync(self) -> dict[str, str]:
        with self._db.lock:
            rows = self._db.conn.execute(
                "SELECT environment, MAX(promoted_at) AS at FROM environments GROUP BY environment"
            ).fetchall()
        return {row["environment"]: row["at"] for row in rows}

    async def list_environments(self) -> list[str]:
        return await self._db.io(self._list_environments_sync)

    def _list_environments_sync(self) -> list[str]:
        with self._db.lock:
            rows = self._db.conn.execute(
                "SELECT DISTINCT environment FROM environments ORDER BY environment"
            ).fetchall()
        return [row["environment"] for row in rows]
