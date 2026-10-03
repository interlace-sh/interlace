"""Cross-process advisory locks stored in the control-plane database.

SQLite takes the lock inside ``BEGIN IMMEDIATE``, which serialises writers.
Postgres does not, so the same decision is one conditional upsert: the row
changes only when this owner already holds it or the lease has expired.
"""

from __future__ import annotations

import time as _time
from datetime import UTC, datetime, timedelta

from interlace.state.conn import ControlDb


class AdvisoryLockStore:
    """Advisory locks on one control-plane connection."""

    def __init__(self, db: ControlDb) -> None:
        self._db = db

    async def acquire_lock(self, name: str, *, owner: str, lease_seconds: float = 180.0, timeout: float = 60.0) -> bool:
        """Take (or renew, if already owned) a named advisory lock. Returns False on timeout."""
        return await self._db.io(self._acquire_lock_sync, name, owner, lease_seconds, timeout)

    def _acquire_lock_sync(self, name: str, owner: str, lease_seconds: float, timeout: float) -> bool:
        deadline = _time.monotonic() + max(0.0, timeout)
        while True:
            if self._try_acquire(name, owner, lease_seconds):
                return True
            if _time.monotonic() >= deadline:
                return False
            _time.sleep(0.05)

    def _try_acquire(self, name: str, owner: str, lease_seconds: float) -> bool:
        with self._db.lock:
            if self._db.dialect == "postgres":
                return self._try_acquire_postgres(name, owner, lease_seconds)
            return self._try_acquire_sqlite(name, owner, lease_seconds)

    def _try_acquire_sqlite(self, name: str, owner: str, lease_seconds: float) -> bool:
        self._db.conn.execute("BEGIN IMMEDIATE")
        try:
            now = datetime.now(UTC)
            row = self._db.conn.execute(
                "SELECT owner, expires_at FROM advisory_locks WHERE name = ?", (name,)
            ).fetchone()
            held = row is not None and datetime.fromisoformat(row["expires_at"]) > now
            if held and row["owner"] != owner:
                self._db.conn.commit()
                return False
            expires = (now + timedelta(seconds=lease_seconds)).isoformat()
            self._db.conn.execute(
                "INSERT INTO advisory_locks (name, owner, expires_at) VALUES (?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET owner = excluded.owner, expires_at = excluded.expires_at",
                (name, owner, expires),
            )
            self._db.conn.commit()
            return True
        except BaseException:
            self._db.conn.rollback()
            raise

    def _try_acquire_postgres(self, name: str, owner: str, lease_seconds: float) -> bool:
        now = datetime.now(UTC)
        expires = (now + timedelta(seconds=lease_seconds)).isoformat()
        # One statement. A lost UPDATE matches nothing, so two sessions cannot both observe a win.
        # Compare the lease as timestamptz: the stored text is ISO-8601, which does not sort as text.
        row = self._db.conn.execute(
            "INSERT INTO advisory_locks (name, owner, expires_at) VALUES (?, ?, ?) "
            "ON CONFLICT(name) DO UPDATE SET owner = excluded.owner, expires_at = excluded.expires_at "
            "WHERE advisory_locks.owner = excluded.owner "
            "OR advisory_locks.expires_at::timestamptz <= ?::timestamptz "
            "RETURNING owner",
            (name, owner, expires, now.isoformat()),
        ).fetchone()
        return row is not None and row["owner"] == owner

    async def renew_lock(self, name: str, *, owner: str, lease_seconds: float = 180.0) -> bool:
        """Extend a lock only if ``owner`` still holds it. Returns False if lost."""
        return await self._db.io(self.renew_lock_sync, name, owner, lease_seconds)

    def renew_lock_sync(self, name: str, owner: str, lease_seconds: float) -> bool:
        """Extend the lock off the event loop. False when ``owner`` no longer holds it."""
        expires = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()
        with self._db.lock:
            cursor = self._db.conn.execute(
                "UPDATE advisory_locks SET expires_at = ? WHERE name = ? AND owner = ?",
                (expires, name, owner),
            )
            self._db.conn.commit()
            return cursor.rowcount > 0

    async def release_lock(self, name: str, *, owner: str) -> None:
        """Drop a lock if ``owner`` still holds it (no-op otherwise)."""
        await self._db.io(self._release_lock_sync, name, owner)

    def _release_lock_sync(self, name: str, owner: str) -> None:
        with self._db.lock:
            self._db.conn.execute("DELETE FROM advisory_locks WHERE name = ? AND owner = ?", (name, owner))
            self._db.conn.commit()
