"""Decode ``pgoutput`` logical-replication messages into row changes.

The slot reader passes each message's WAL start as ``lsn``. Relation messages
update ``relations`` and return None. Insert, update, and delete return a
:class:`Change` whose ``row`` is text values keyed by the relation's column names.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any


@dataclass
class Relation:
    namespace: str
    name: str
    columns: list[str]

    @property
    def qualified(self) -> str:
        return f"{self.namespace}.{self.name}"


@dataclass
class Change:
    """One row change. ``kind`` is ``insert``, ``update``, or ``delete``."""

    lsn: str
    kind: str
    table: str
    row: dict[str, Any]
    before: dict[str, Any] | None = None


@dataclass
class _Cursor:
    data: bytes
    pos: int = 0

    def byte(self) -> bytes:
        value = self.data[self.pos : self.pos + 1]
        self.pos += 1
        return value

    def i16(self) -> int:
        value = struct.unpack_from("!h", self.data, self.pos)[0]
        self.pos += 2
        return int(value)

    def i32(self) -> int:
        value = struct.unpack_from("!i", self.data, self.pos)[0]
        self.pos += 4
        return int(value)

    def i64(self) -> int:
        value = struct.unpack_from("!q", self.data, self.pos)[0]
        self.pos += 8
        return int(value)

    def text(self) -> str:
        end = self.data.index(b"\x00", self.pos)
        value = self.data[self.pos : end].decode()
        self.pos = end + 1
        return value

    def tuple(self) -> list[str | None]:
        values: list[str | None] = []
        for _ in range(self.i16()):
            kind = self.byte()
            if kind in (b"n", b"u"):
                values.append(None)
                continue
            if kind != b"t":
                raise ValueError(f"unsupported pgoutput tuple marker {kind!r}")
            length = self.i32()
            values.append(self.data[self.pos : self.pos + length].decode())
            self.pos += length
        return values


def decode_message(payload: bytes, relations: dict[int, Relation], lsn: str) -> Change | None:
    """Decode one ``pgoutput`` message. Begin, commit, and relation messages return None."""
    if not payload:
        return None
    cur = _Cursor(payload)
    kind = cur.byte()
    if kind == b"R":
        oid = cur.i32()
        namespace = cur.text()
        name = cur.text()
        cur.byte()  # replica identity
        columns: list[str] = []
        for _ in range(cur.i16()):
            cur.byte()  # flags
            columns.append(cur.text())
            cur.i32()  # type oid
            cur.i32()  # typemod
        relations[oid] = Relation(namespace, name, columns)
        return None
    if kind == b"I":
        return _row_change(cur, relations, lsn, "insert")
    if kind == b"U":
        return _update(cur, relations, lsn)
    if kind == b"D":
        return _row_change(cur, relations, lsn, "delete")
    return None  # begin, commit, truncate, type, origin: no row


def _relation(cur: _Cursor, relations: dict[int, Relation]) -> Relation:
    oid = cur.i32()
    found = relations.get(oid)
    if found is None:
        raise ValueError(f"pgoutput message for unknown relation oid {oid}")
    return found


def _as_row(relation: Relation, values: list[str | None]) -> dict[str, Any]:
    return dict(zip(relation.columns, values, strict=False))


def _row_change(cur: _Cursor, relations: dict[int, Relation], lsn: str, kind: str) -> Change:
    relation = _relation(cur, relations)
    cur.byte()  # 'N' for insert, 'K' or 'O' for delete
    return Change(lsn=lsn, kind=kind, table=relation.qualified, row=_as_row(relation, cur.tuple()))


def _update(cur: _Cursor, relations: dict[int, Relation], lsn: str) -> Change:
    relation = _relation(cur, relations)
    marker = cur.byte()
    before: dict[str, Any] | None = None
    if marker in (b"K", b"O"):
        before = _as_row(relation, cur.tuple())
        marker = cur.byte()
    if marker != b"N":
        raise ValueError(f"pgoutput update expected a new tuple, got {marker!r}")
    return Change(
        lsn=lsn,
        kind="update",
        table=relation.qualified,
        row=_as_row(relation, cur.tuple()),
        before=before,
    )


def payload_of(change: Change) -> dict[str, Any]:
    """Stream payload: the row, plus ``_change`` so a merge model can drop deletes."""
    body = dict(change.row)
    body["_change"] = change.kind
    body["_table"] = change.table
    return body
