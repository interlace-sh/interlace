"""Replication LSN confirmed only after the stream watermark advances.

The rows live in the control-plane database, beside the snapshots, but they are
not part of the plan/apply protocol. CDC publish and the serve loop are the
only callers.
"""

from __future__ import annotations

from interlace.state.conn import ControlDb


class CdcWatermarks:
    """Confirmed and pending replication LSNs on one control-plane connection."""

    def __init__(self, db: ControlDb) -> None:
        self._db = db

    async def cdc_confirmed_lsn(self, stream: str) -> str | None:
        """The replication LSN whose rows have been flushed, or None if CDC has not confirmed one."""
        return await self._db.io(self._cdc_confirmed_lsn_sync, stream)

    def _cdc_confirmed_lsn_sync(self, stream: str) -> str | None:
        with self._db.lock:
            row = self._db.conn.execute("SELECT lsn FROM cdc_confirmed WHERE stream = ?", (stream,)).fetchone()
        return str(row["lsn"]) if row else None

    async def cdc_note_pending(self, stream: str, rows: list[tuple[int, str]]) -> None:
        """Remember which log offset each replication LSN landed at. Deduped appends are included."""
        await self._db.io(self._cdc_note_pending_sync, stream, rows)

    def _cdc_note_pending_sync(self, stream: str, rows: list[tuple[int, str]]) -> None:
        with self._db.lock:
            self._db.conn.executemany(
                "INSERT OR REPLACE INTO cdc_pending (stream, log_offset, lsn) VALUES (?, ?, ?)",
                [(stream, offset, lsn) for offset, lsn in rows],
            )
            self._db.conn.commit()

    async def cdc_advance(self, stream: str, watermark: int) -> str | None:
        """Confirm the LSN of every pending row at or below ``watermark``.

        ``watermark`` is the offset ``flush_streams`` has committed. Rows past it
        stay pending, so a crash re-reads from the last confirmed LSN.
        """
        return await self._db.io(self._cdc_advance_sync, stream, watermark)

    def _cdc_advance_sync(self, stream: str, watermark: int) -> str | None:
        with self._db.lock:
            row = self._db.conn.execute(
                "SELECT lsn FROM cdc_pending WHERE stream = ? AND log_offset <= ? ORDER BY log_offset DESC LIMIT 1",
                (stream, watermark),
            ).fetchone()
            if row is None:
                return self._cdc_confirmed_lsn_unlocked(stream)
            lsn = str(row["lsn"])
            self._db.conn.execute(
                "DELETE FROM cdc_pending WHERE stream = ? AND log_offset <= ?",
                (stream, watermark),
            )
            self._db.conn.execute(
                "INSERT OR REPLACE INTO cdc_confirmed (stream, lsn) VALUES (?, ?)",
                (stream, lsn),
            )
            self._db.conn.commit()
        return lsn

    def _cdc_confirmed_lsn_unlocked(self, stream: str) -> str | None:
        row = self._db.conn.execute("SELECT lsn FROM cdc_confirmed WHERE stream = ?", (stream,)).fetchone()
        return str(row["lsn"]) if row else None
