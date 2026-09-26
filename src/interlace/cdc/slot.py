"""Read a Postgres logical replication slot with ``psycopg`` and decode ``pgoutput``.

psycopg 3 has no replication cursor, so this peeks the slot with
``pg_logical_slot_peek_binary_changes`` (Postgres 14+) and does not move it.
:meth:`SlotReader.feedback` advances the slot only after
:func:`interlace.cdc.publish.confirm_flushed` has stored the LSN — never when
the message is merely appended to the stream log.
"""

from __future__ import annotations

from typing import Any

from interlace.cdc.decode import Change, Relation, decode_message
from interlace.config.config import CdcConfig
from interlace.exceptions import ConfigurationError


class SlotReader:
    """One Postgres connection for one slot. ``poll`` does not advance the slot."""

    def __init__(self, dsn: str, source: CdcConfig) -> None:
        self._dsn = dsn
        self._source = source
        self._conn: Any = None
        self._relations: dict[int, Relation] = {}
        self._tables = set(source.tables)

    def poll(self, confirmed_lsn: str | None, *, limit: int = 100) -> list[Change]:
        """Read up to ``limit`` decoded row changes. Returns immediately when the slot is idle.

        ``confirmed_lsn`` is the highest LSN already flushed. Changes at or below it are
        skipped, so a peek that still contains them (feedback has not advanced the slot yet)
        does not publish them again.
        """
        confirmed = _lsn_key(confirmed_lsn) if confirmed_lsn else None
        changes: list[Change] = []
        # Begin, relation, and commit messages sit beside each row change.
        for lsn, data in self._peek(max(limit * 4, limit)):
            payload = bytes(data)
            change = decode_message(payload, self._relations, lsn)
            if change is None or not self._wanted(change.table):
                continue
            if confirmed is not None and _lsn_key(change.lsn) <= confirmed:
                continue
            changes.append(change)
            if len(changes) >= limit:
                break
        return changes

    def feedback(self, lsn: str) -> None:
        """Advance the slot to ``lsn``. Safe to call only after confirm_flushed."""
        if not lsn or self._conn is None:
            return
        self._fetch("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (self._source.slot, lsn))

    def close(self) -> None:
        conn = self._conn
        self._conn = None
        close = getattr(conn, "close", None)
        if close is not None:
            close()

    def _wanted(self, table: str) -> bool:
        if table in self._tables:
            return True
        return any(item == table.rsplit(".", 1)[-1] for item in self._tables)

    def _peek(self, nchanges: int) -> list[tuple[str, bytes]]:
        rows = self._fetch(
            """
            SELECT lsn::text, data
            FROM pg_logical_slot_peek_binary_changes(
                %s, NULL, %s, 'proto_version', '1', 'publication_names', %s
            )
            """,
            (self._source.slot, nchanges, self._source.publication),
        )
        return [(str(row[0]), bytes(row[1])) for row in rows]

    def _fetch(self, sql: str, params: tuple[object, ...]) -> list[Any]:
        conn = self._connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())
        except Exception as exc:
            if "pg_logical_slot_peek_binary_changes" in str(exc):
                raise ConfigurationError(
                    "CDC needs Postgres 14 or newer (pg_logical_slot_peek_binary_changes)."
                ) from exc
            raise

    def _connection(self) -> Any:
        if self._conn is not None:
            return self._conn
        try:
            import psycopg  # optional postgres extra
        except ImportError as exc:
            raise ConfigurationError("CDC needs the postgres extra (psycopg). Install interlaced[postgres].") from exc
        self._conn = psycopg.connect(self._dsn, autocommit=True)
        return self._conn


def _lsn_key(lsn: str) -> tuple[int, int]:
    """Order key for a ``pg_lsn`` text value (``high/low``, both hex)."""
    high, sep, low = lsn.partition("/")
    if not sep:
        raise ValueError(f"not a pg_lsn: {lsn!r}")
    return (int(high, 16), int(low, 16))
