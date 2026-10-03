"""Control-plane event log (run and apply lifecycle, SSE replay)."""

from __future__ import annotations

import json
import os

from interlace.state.codec import _now_iso, _stamp_actor
from interlace.state.conn import ControlDb


class EventLogStore:
    """The control-plane event log on one control-plane connection."""

    def __init__(self, db: ControlDb) -> None:
        self._db = db
        self.path: str | None = None

    async def events_for_entity(self, entity: str) -> list[dict[str, object]]:
        return await self._db.io(self._events_for_entity_sync, entity)

    async def latest_model_build(self, model: str) -> dict[str, object] | None:
        """The newest terminal build event for a model (done / failed / cancelled)."""
        return await self._db.io(self._latest_model_build_sync, model)

    def _latest_model_build_sync(self, model: str) -> dict[str, object] | None:
        with self._db.lock:
            row = self._db.conn.execute(
                "SELECT seq, ts, type, entity, payload FROM event_log "
                "WHERE entity = ? AND type IN ('model.done', 'model.failed', 'model.cancelled') "
                "ORDER BY seq DESC LIMIT 1",
                (model,),
            ).fetchone()
        return self._event_row(row) if row else None

    def _events_for_entity_sync(self, entity: str) -> list[dict[str, object]]:
        with self._db.lock:
            rows = self._db.conn.execute(
                "SELECT seq, ts, type, entity, payload FROM event_log WHERE entity = ? ORDER BY seq", (entity,)
            ).fetchall()
        return [self._event_row(row) for row in rows]

    async def events_for_run(self, run_id: int) -> list[dict[str, object]]:
        """A run's per-model events — keyed by ``payload.run`` (their entity is the model
        name, not the run id), so the run detail can show a model-level timeline."""
        return await self._db.io(self._events_for_run_sync, run_id)

    def _events_for_run_sync(self, run_id: int) -> list[dict[str, object]]:
        with self._db.lock:
            rows = self._db.conn.execute(
                "SELECT seq, ts, type, entity, payload FROM event_log "
                "WHERE CAST(json_extract(payload, '$.run') AS TEXT) = ? ORDER BY seq",
                (str(run_id),),
            ).fetchall()
        return [self._event_row(row) for row in rows]

    @staticmethod
    def _event_row(row: object) -> dict[str, object]:
        return {
            "seq": row["seq"],  # type: ignore[index]
            "ts": row["ts"],  # type: ignore[index]
            "type": row["type"],  # type: ignore[index]
            "entity": row["entity"],  # type: ignore[index]
            "payload": json.loads(row["payload"]) if row["payload"] else None,  # type: ignore[index]
        }

    async def append_event(self, type: str, entity: str | None = None, payload: dict[str, object] | None = None) -> int:
        return await self._db.io(self._append_event_sync, type, entity, payload)

    def _append_event_sync(self, type: str, entity: str | None, payload: dict[str, object] | None) -> int:
        payload = _stamp_actor(type, payload)
        ts = _now_iso()
        with self._db.lock:
            cursor = self._db.conn.execute(
                "INSERT INTO event_log (ts, type, entity, payload) VALUES (?, ?, ?, ?)",
                (ts, type, entity, json.dumps(payload) if payload is not None else None),
            )
            self._db.conn.commit()
            seq = int(cursor.lastrowid or 0)
        self._mirror_event(seq, ts, type, entity, payload)
        return seq

    def _mirror_event(
        self, seq: int, ts: str, type: str, entity: str | None, payload: dict[str, object] | None
    ) -> None:
        """One NDJSON line after the SQLite commit, when ``event_log_path`` is set."""
        path = self.path
        if not path:
            return
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        line = json.dumps(
            {"seq": seq, "ts": ts, "type": type, "entity": entity, "payload": payload},
            default=str,
            separators=(",", ":"),
        )
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()

    async def latest_event_seq(self) -> int:
        """The event log's current head (0 when empty) — where a live tail starts."""
        return await self._db.io(self._latest_event_seq_sync)

    def _latest_event_seq_sync(self) -> int:
        with self._db.lock:
            row = self._db.conn.execute("SELECT max(seq) FROM event_log").fetchone()
        return int(row[0] or 0)

    async def read_events(self, after_seq: int = 0, limit: int = 200) -> list[dict[str, object]]:
        return await self._db.io(self._read_events_sync, after_seq, limit)

    def _read_events_sync(self, after_seq: int, limit: int) -> list[dict[str, object]]:
        with self._db.lock:
            rows = self._db.conn.execute(
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
