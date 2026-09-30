"""Cross-process advisory locks stored in the control-plane database."""

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
            with self._db.lock:
                self._db.conn.execute("BEGIN IMMEDIATE")
                try:
                    now = datetime.now(UTC)
                    row = self._db.conn.execute(
                        "SELECT owner, expires_at FROM advisory_locks WHERE name = ?", (name,)
                    ).fetchone()
                    held = row is not None and datetime.fromisoformat(row["expires_at"]) > now
                    if held and row["owner"] != owner:
                        self._db.conn.commit()
                    else:
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
            if _time.monotonic() >= deadline:
                return False
            _time.sleep(0.05)

    async def renew_lock(self, name: str, *, owner: str, lease_seconds: float = 180.0) -> bool:
        """Extend a lock only if ``owner`` still holds it. Returns False if lost."""
        return await self._db.io(self.renew_lock_sync, name, owner, lease_seconds)

    def renew_lock_sync(self, name: str, owner: str, lease_seconds: float) -> bool:
        """Extend the lock off the event loop. False when ``owner`` no longer holds it."""
        with self._db.lock:
            self._db.conn.execute("BEGIN IMMEDIATE")
            try:
                now = datetime.now(UTC)
                row = self._db.conn.execute(
                    "SELECT owner, expires_at FROM advisory_locks WHERE name = ?", (name,)
                ).fetchone()
                # An expired row we still own has not been stolen. Extending it is what
                # lets a late heartbeat recover; a thief already replaced ``owner``.
                if row is None or row["owner"] != owner:
                    self._db.conn.commit()
                    return False
                expires = (now + timedelta(seconds=lease_seconds)).isoformat()
                self._db.conn.execute(
                    "UPDATE advisory_locks SET expires_at = ? WHERE name = ? AND owner = ?",
                    (expires, name, owner),
                )
                self._db.conn.commit()
                return True
            except BaseException:
                self._db.conn.rollback()
                raise

    async def release_lock(self, name: str, *, owner: str) -> None:
        """Drop a lock if ``owner`` still holds it (no-op otherwise)."""
        await self._db.io(self._release_lock_sync, name, owner)

    def _release_lock_sync(self, name: str, owner: str) -> None:
        with self._db.lock:
            self._db.conn.execute("DELETE FROM advisory_locks WHERE name = ? AND owner = ?", (name, owner))
            self._db.conn.commit()
