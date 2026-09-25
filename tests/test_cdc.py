"""Postgres CDC: pgoutput decoding, and an LSN that moves only after a flush."""

from __future__ import annotations

import os
import struct
from pathlib import Path

import pytest

from interlace.cdc.decode import Relation, decode_message
from interlace.cdc.publish import confirm_flushed, publish_changes
from interlace.state.store import SqliteStateStore
from interlace.streaming.log import SqliteStreamLog

pytestmark = pytest.mark.unit


def _column(name: str, oid: int) -> bytes:
    return b"\x00" + name.encode() + b"\x00" + struct.pack("!ii", oid, -1)


def _text(value: str) -> bytes:
    raw = value.encode()
    return b"t" + struct.pack("!i", len(raw)) + raw


def test_pgoutput_insert_and_delete_become_rows() -> None:
    relations: dict[int, Relation] = {}
    relation = (
        b"R"
        + struct.pack("!i", 10)
        + b"public\x00orders\x00d"
        + struct.pack("!h", 2)
        + _column("id", 23)
        + _column("status", 25)
    )
    assert decode_message(relation, relations, "0/1") is None
    inserted = decode_message(
        b"I" + struct.pack("!i", 10) + b"N" + struct.pack("!h", 2) + _text("7") + _text("open"),
        relations,
        "0/2",
    )
    assert inserted is not None
    assert inserted.kind == "insert"
    assert inserted.row == {"id": "7", "status": "open"}
    deleted = decode_message(
        b"D" + struct.pack("!i", 10) + b"K" + struct.pack("!h", 1) + _text("7"),
        relations,
        "0/3",
    )
    assert deleted is not None
    assert deleted.kind == "delete"
    assert deleted.row["id"] == "7"


async def test_lsn_advances_only_after_the_watermark(tmp_path: Path) -> None:
    from interlace.cdc.decode import Change

    log = await SqliteStreamLog.open(tmp_path / "streams.db")
    store = await SqliteStateStore.open(tmp_path / "state.db")
    changes = [
        Change(lsn="0/10", kind="insert", table="public.orders", row={"id": "1"}),
        Change(lsn="0/11", kind="delete", table="public.orders", row={"id": "1"}),
    ]
    offsets = await publish_changes(log, store, "orders", changes)
    assert await store.cdc_confirmed_lsn("orders") is None
    assert await confirm_flushed(store, "orders", offsets[0] - 1) is None
    assert await confirm_flushed(store, "orders", offsets[0]) == "0/10"
    assert await store.cdc_confirmed_lsn("orders") == "0/10"
    assert await confirm_flushed(store, "orders", offsets[1]) == "0/11"

    again = await publish_changes(log, store, "orders", changes[:1])
    assert again == [offsets[0]]  # the same LSN is deduped
    stored = await log.read("orders", 0, 10)
    assert [event.payload["_change"] for event in stored] == ["insert", "delete"]
    await log.close()
    await store.close()


DSN = os.environ.get("INTERLACE_TEST_PG_DSN", "postgresql://postgres:pg@localhost:5455/postgres")


def _logical_wal() -> bool:
    try:
        import psycopg
    except ImportError:
        return False
    try:
        with psycopg.connect(DSN, connect_timeout=2) as conn, conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            row = cur.fetchone()
        return bool(row and row[0] == "logical")
    except Exception:
        return False


@pytest.mark.requires_db
async def test_live_slot_appends_and_confirms_after_flush(tmp_path: Path) -> None:
    """A real slot, skipped unless Postgres is up with wal_level=logical."""
    pytest.importorskip("psycopg")
    if not _logical_wal():
        pytest.skip("Postgres is not reachable with wal_level=logical")
    import psycopg

    from interlace.cdc.slot import SlotReader
    from interlace.config.config import CdcConfig

    slot = "interlace_cdc_test"
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("DROP PUBLICATION IF EXISTS interlace_cdc_pub")
        cur.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s", (slot,)
        )
        cur.execute("CREATE TABLE IF NOT EXISTS cdc_probe (id int primary key, status text)")
        cur.execute("TRUNCATE cdc_probe")
        cur.execute("CREATE PUBLICATION interlace_cdc_pub FOR TABLE cdc_probe")
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
        cur.execute("INSERT INTO cdc_probe (id, status) VALUES (1, 'open')")
    source = CdcConfig(
        connection="src",
        slot=slot,
        publication="interlace_cdc_pub",
        tables=["public.cdc_probe"],
        stream="probe",
    )
    reader = SlotReader(DSN, source)
    log = await SqliteStreamLog.open(tmp_path / "streams.db")
    store = await SqliteStateStore.open(tmp_path / "state.db")
    try:
        changes = reader.poll(None, limit=20)
        assert any(change.kind == "insert" and change.row.get("id") == "1" for change in changes)
        offsets = await publish_changes(log, store, "probe", changes)
        assert await store.cdc_confirmed_lsn("probe") is None
        assert await confirm_flushed(store, "probe", max(offsets)) == changes[-1].lsn
    finally:
        reader.close()
        await log.close()
        await store.close()
        with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
                (slot,),
            )
            cur.execute("DROP PUBLICATION IF EXISTS interlace_cdc_pub")
