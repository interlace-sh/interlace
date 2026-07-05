"""Durable streaming: the SQLite stream log, schema validation, the exactly-once
materializer, and the HTTP publish path."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import sqlglot

from interlace.dsl.decorators import StreamDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import StreamError
from interlace.streaming.log import Event, SqliteStreamLog
from interlace.streaming.materializer import ensure_stream_tables, flush_stream, stream_watermark
from interlace.streaming.schema import arrow_schema, validate_rows

pytestmark = pytest.mark.unit

CLICKS = StreamDef(name="clicks", schema={"user": "string", "amount": "double"}, idempotency_key="event_id")
CLICKS_PLAIN = StreamDef(
    name="clicks", schema={"event_id": "string", "user": "string", "amount": "double", "at": "timestamp"}
)


@pytest.fixture()
async def log(tmp_path: Path) -> AsyncIterator[SqliteStreamLog]:
    stream_log = await SqliteStreamLog.open(tmp_path / "streams.db")
    yield stream_log
    await stream_log.close()


# --- log ----------------------------------------------------------------------


async def test_append_assigns_monotonic_offsets_and_survives_reopen(tmp_path: Path) -> None:
    log = await SqliteStreamLog.open(tmp_path / "streams.db")
    result = await log.append("s", [Event({"n": 1}), Event({"n": 2})])
    assert result.offsets == [1, 2] and result.deduped == [False, False]
    await log.close()

    reopened = await SqliteStreamLog.open(tmp_path / "streams.db")  # durability
    assert await reopened.head("s") == 2
    events = await reopened.read("s", 0, 10)
    assert [e.payload["n"] for e in events] == [1, 2]
    more = await reopened.append("s", [Event({"n": 3})])
    assert more.offsets == [1 + 2]  # offset sequence continues across restarts
    await reopened.close()


async def test_idempotency_key_deduplicates(log: SqliteStreamLog) -> None:
    first = await log.append("s", [Event({"n": 1}, idempotency_key="a")])
    retry = await log.append("s", [Event({"n": 1}, idempotency_key="a"), Event({"n": 2}, idempotency_key="b")])
    assert retry.deduped == [True, False]
    assert retry.offsets[0] == first.offsets[0]  # duplicate reports the original offset
    assert await log.head("s") == 2


async def test_lease_fencing(log: SqliteStreamLog) -> None:
    lease = await log.lease("s", "mat", ttl=60, owner="w1")
    assert lease is not None and lease.committed_offset == 0
    assert await log.lease("s", "mat", ttl=60, owner="w2") is None  # held

    await log.commit("s", "mat", 5, lease.token)
    renewed = await log.lease("s", "mat", ttl=60, owner="w1")  # same owner re-acquires
    assert renewed is not None and renewed.committed_offset == 5

    with pytest.raises(StreamError, match="stale lease"):
        await log.commit("s", "mat", 9, lease.token)  # old fencing token rejected


async def test_trim(log: SqliteStreamLog) -> None:
    await log.append("s", [Event({"n": i}) for i in range(5)])
    assert await log.trim("s") == 0  # refuses a bare trim
    assert await log.trim("s", before_offset=4) == 3
    assert [e.offset for e in await log.read("s", 0, 10)] == [4, 5]
    assert await log.head("s") == 5  # offsets never rewind


# --- schema -------------------------------------------------------------------


def test_validate_rejects_drift_and_wrong_types() -> None:
    validate_rows(CLICKS_PLAIN, [{"event_id": "e1", "user": "u", "amount": 1.5}])
    validate_rows(CLICKS_PLAIN, [{"event_id": "e1"}])  # missing fields load as NULL
    with pytest.raises(StreamError, match="undeclared fields"):
        validate_rows(CLICKS_PLAIN, [{"event_id": "e1", "extra": 1}])
    with pytest.raises(StreamError, match="must be double"):
        validate_rows(CLICKS_PLAIN, [{"amount": "not-a-number"}])
    with pytest.raises(StreamError, match="must be string"):
        validate_rows(CLICKS_PLAIN, [{"user": True}])


def test_arrow_schema_appends_ingestion_metadata() -> None:
    schema = arrow_schema(CLICKS_PLAIN)
    assert schema.names == ["event_id", "user", "amount", "at", "_offset", "_ingested_at"]
    with pytest.raises(StreamError, match="unknown type"):
        arrow_schema(StreamDef(name="bad", schema={"x": "geometry"}))


# --- materializer ---------------------------------------------------------------


async def _rows(engine: DuckDBAdapter, sql: str) -> list[dict]:
    return (await engine.fetch(sqlglot.parse_one(sql))).read_all().to_pylist()


async def test_flush_is_exactly_once(log: SqliteStreamLog) -> None:
    engine = DuckDBAdapter.in_memory()
    await ensure_stream_tables([CLICKS_PLAIN], engine)

    await log.append("clicks", [Event({"event_id": "e1", "user": "u1", "amount": 5.0, "at": "2026-07-04T10:00:00"})])
    assert await flush_stream(CLICKS_PLAIN, log, engine) == 1
    assert await flush_stream(CLICKS_PLAIN, log, engine) == 0  # nothing new: no duplicates
    assert await stream_watermark(CLICKS_PLAIN, engine) == 1

    await log.append("clicks", [Event({"event_id": "e2", "user": "u2", "amount": 7.5})])
    assert await flush_stream(CLICKS_PLAIN, log, engine) == 1

    rows = await _rows(engine, "SELECT event_id, user, amount, _offset FROM streams.clicks ORDER BY _offset")
    assert rows == [
        {"event_id": "e1", "user": "u1", "amount": 5.0, "_offset": 1},
        {"event_id": "e2", "user": "u2", "amount": 7.5, "_offset": 2},
    ]
    engine.close()


async def test_sql_model_reads_stream_table(log: SqliteStreamLog, tmp_path: Path) -> None:
    """The full loop: publish -> materialize -> a SQL model aggregates streams.<name>."""
    from interlace.dsl.decorators import ModelDef
    from interlace.graph.project import compile_models
    from interlace.plan.apply import apply
    from interlace.plan.differ import diff
    from interlace.state.store import SqliteStateStore

    engine = DuckDBAdapter.in_memory()
    await ensure_stream_tables([CLICKS_PLAIN], engine)
    await log.append("clicks", [Event({"event_id": f"e{i}", "user": "u", "amount": float(i)}) for i in range(3)])
    await flush_stream(CLICKS_PLAIN, log, engine)

    store = await SqliteStateStore.open(tmp_path / "state.db")
    compiled = compile_models(
        [ModelDef(name="click_totals", sql="SELECT user, sum(amount) AS total FROM streams.clicks GROUP BY user")]
    )
    await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)
    assert await _rows(engine, "SELECT total FROM dev__main.click_totals") == [{"total": 3.0}]
    await store.close()
    engine.close()
