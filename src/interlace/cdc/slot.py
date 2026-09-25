"""Read a Postgres logical replication slot with ``psycopg`` and decode ``pgoutput``.

The connection is opened only when CDC is configured. Feedback to the slot
(which lets Postgres drop WAL) is sent by :meth:`SlotReader.feedback` after
:func:`interlace.cdc.publish.confirm_flushed` has stored the LSN — never when
the message is merely appended to the stream log.
"""

from __future__ import annotations

from typing import cast

from interlace.cdc.decode import Change, Relation, decode_message
from interlace.config.config import CdcConfig
from interlace.exceptions import ConfigurationError


class SlotReader:
    """One replication connection for one slot. ``poll`` does not advance the slot."""

    def __init__(self, dsn: str, source: CdcConfig) -> None:
        self._dsn = dsn
        self._source = source
        self._conn: object | None = None
        self._cur: object | None = None
        self._relations: dict[int, Relation] = {}
        self._started = False
        self._tables = set(source.tables)

    def poll(self, confirmed_lsn: str | None, *, limit: int = 100) -> list[Change]:
        """Read up to ``limit`` decoded row changes. Blocks briefly when the slot is idle."""
        cursor = self._cursor(confirmed_lsn)
        changes: list[Change] = []
        while len(changes) < limit:
            message = self._read_one(cursor)
            if message is None:
                break
            payload = getattr(message, "payload", b"")
            data = payload if isinstance(payload, bytes) else bytes(payload)
            lsn = str(getattr(message, "data_start", ""))
            change = decode_message(data, self._relations, lsn)
            if change is None or not self._wanted(change.table):
                continue
            changes.append(change)
        return changes

    def feedback(self, lsn: str) -> None:
        """Tell the slot that ``lsn`` is flushed. Safe to call only after confirm_flushed."""
        cursor = self._cur
        if cursor is None or not lsn:
            return
        send = getattr(cursor, "send_feedback", None)
        if send is not None:
            send(flush_lsn=lsn, force=True)

    def close(self) -> None:
        conn = self._conn
        self._cur = None
        self._conn = None
        self._started = False
        close = getattr(conn, "close", None)
        if close is not None:
            close()

    def _wanted(self, table: str) -> bool:
        if table in self._tables:
            return True
        return any(item == table.rsplit(".", 1)[-1] for item in self._tables)

    def _cursor(self, confirmed_lsn: str | None) -> object:
        if self._cur is not None:
            return self._cur
        try:
            import psycopg  # optional postgres extra
        except ImportError as exc:
            raise ConfigurationError("CDC needs the postgres extra (psycopg). Install interlaced[postgres].") from exc
        conn = psycopg.connect(self._dsn, autocommit=True, replication="database")
        cursor = conn.cursor()
        options = {"proto_version": "1", "publication_names": self._source.publication}
        start = confirmed_lsn if confirmed_lsn else None
        cursor.start_replication(slot_name=self._source.slot, decode=False, start_lsn=start, options=options)
        self._conn = conn
        self._cur = cursor
        self._started = True
        return cursor

    def _read_one(self, cursor: object) -> object | None:
        read = getattr(cursor, "read_message", None)
        if read is None:
            raise ConfigurationError("this psycopg build has no replication cursor")
        try:
            return cast(object | None, read(timeout=0.2))
        except Exception as exc:
            # psycopg raises a timeout-ish error or returns None when the slot is idle
            if type(exc).__name__ in {"ReplicationTimeout", "QueryCanceled"} or "timeout" in str(exc).lower():
                return None
            raise
