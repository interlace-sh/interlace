"""Persisted data-quality check results."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from interlace.state.codec import _now_iso
from interlace.state.conn import ControlDb


class CheckStore:
    """Check results on one control-plane connection."""

    def __init__(self, db: ControlDb) -> None:
        self._db = db

    async def record_check_results(self, environment: str, fingerprint: str, outcomes: Iterable[Any]) -> None:
        """Persist one model's check outcomes (objects with name/type/severity/status/failures/message)."""
        await self._db.io(self._record_check_results_sync, environment, fingerprint, list(outcomes))

    def _record_check_results_sync(self, environment: str, fingerprint: str, outcomes: list[Any]) -> None:
        now = _now_iso()
        with self._db.lock:
            self._db.conn.executemany(
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
            self._db.conn.commit()

    async def list_check_results(self, model: str | None = None, limit: int = 200) -> list[dict[str, object]]:
        return await self._db.io(self._list_check_results_sync, model, limit)

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
        with self._db.lock:
            rows = self._db.conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]
