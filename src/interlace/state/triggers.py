"""Last-fired time for each scheduler trigger."""

from __future__ import annotations

from datetime import datetime

from interlace.state.conn import ControlDb


class TriggerStore:
    """Trigger fire times on one control-plane connection."""

    def __init__(self, db: ControlDb) -> None:
        self._db = db

    async def get_trigger_last_fired(self, trigger_id: str) -> datetime | None:
        return await self._db.io(self._get_trigger_last_fired_sync, trigger_id)

    def _get_trigger_last_fired_sync(self, trigger_id: str) -> datetime | None:
        with self._db.lock:
            row = self._db.conn.execute(
                "SELECT last_fired_at FROM trigger_state WHERE trigger_id = ?", (trigger_id,)
            ).fetchone()
        return datetime.fromisoformat(row["last_fired_at"]) if row and row["last_fired_at"] else None

    async def set_trigger_last_fired(self, trigger_id: str, when: datetime) -> None:
        await self._db.io(self._set_trigger_last_fired_sync, trigger_id, when)

    def _set_trigger_last_fired_sync(self, trigger_id: str, when: datetime) -> None:
        with self._db.lock:
            self._db.conn.execute(
                "INSERT OR REPLACE INTO trigger_state (trigger_id, last_fired_at) VALUES (?, ?)",
                (trigger_id, when.isoformat()),
            )
            self._db.conn.commit()
