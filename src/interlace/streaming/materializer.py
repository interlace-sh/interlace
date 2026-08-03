"""The stream materializer — micro-batches from the log into the warehouse.

A flush drains everything past the stream's watermark in ``batch_rows`` chunks;
each chunk stages one Arrow batch and moves ``stage -> target table + watermark``
in a single engine transaction. Crash anywhere leaves either the old watermark
(events re-read, stage overwritten — no duplicates) or the new one —
**exactly-once into the warehouse** without coordinating with the log. The
watermark lives in the warehouse (``streams._watermarks``) precisely so it
commits atomically with the data; the log's consumer-group lease/commit
machinery is for external consumers, not this path.

Stream tables land in the ``streams`` schema (``streams.<name>``) with the
declared fields plus ``_offset`` and ``_ingested_at``, so SQL models simply
``FROM streams.<name>``.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime

import pyarrow as pa
from sqlglot import exp, parse_one

from interlace.dsl.decorators import StreamDef
from interlace.engines.base import EngineAdapter
from interlace.graph.project import CompiledProject
from interlace.ir.relation import TableRef
from interlace.state.interval import parse_grain
from interlace.streaming.log import StreamLog
from interlace.streaming.schema import arrow_schema, coerce_row, evolved_columns, sql_columns

_SCHEMA = "streams"
_WATERMARKS = TableRef(schema=_SCHEMA, name="_watermarks")


def target_table(stream: StreamDef) -> TableRef:
    return TableRef(schema=_SCHEMA, name=stream.name)


def _sql(table: TableRef) -> str:
    return exp.table_(table.name, db=table.schema).sql(dialect="duckdb")


async def ensure_stream_tables(streams: Iterable[StreamDef], engine: EngineAdapter) -> None:
    """Create the streams schema, watermark table, and one table per stream."""
    await engine.create_schema(_SCHEMA)
    await engine.execute_sql(f"CREATE TABLE IF NOT EXISTS {_sql(_WATERMARKS)} (stream TEXT, committed_offset BIGINT)")
    for stream in streams:
        columns = ", ".join(
            f"{exp.column(name).sql(dialect='duckdb', identify=True)} {sql_type}"  # quoted: names may be keywords
            for name, sql_type in sql_columns(stream)
        )
        await engine.execute_sql(f"CREATE TABLE IF NOT EXISTS {_sql(target_table(stream))} ({columns})")


async def stream_watermark(stream: StreamDef, engine: EngineAdapter) -> int:
    reader = await engine.fetch_sql(
        f"SELECT max(committed_offset) AS offset FROM {_sql(_WATERMARKS)} WHERE stream = '{stream.name}'"
    )
    rows = reader.read_all().to_pylist()
    return int(rows[0]["offset"] or 0) if rows else 0


_ARROW_BY_SQL = {"BIGINT": pa.int64(), "DOUBLE": pa.float64(), "BOOLEAN": pa.bool_(), "TEXT": pa.string()}


def quarantine_stream(stream: StreamDef) -> StreamDef:
    """The shadow stream that receives a quarantine-mode stream's failing events."""
    return StreamDef(
        name=f"{stream.name}__quarantine", schema={"error": "text", "payload": "json"}, retention=stream.retention
    )


async def flush_stream(stream: StreamDef, log: StreamLog, engine: EngineAdapter, *, batch_rows: int = 5000) -> int:
    """Drain everything durable for ``stream`` into the warehouse in
    ``batch_rows`` micro-batches; returns rows materialized. Draining (not a
    single batch) is what lets callers — the flusher, an apply's pre-flush,
    shutdown — assume the warehouse has caught up with the log when this
    returns."""
    total = 0
    watermark = await stream_watermark(stream, engine)
    while True:
        count, watermark = await _flush_batch(stream, log, engine, watermark, batch_rows)
        total += count
        if count < batch_rows:  # short batch: the log is drained
            return total


async def _flush_batch(
    stream: StreamDef, log: StreamLog, engine: EngineAdapter, watermark: int, batch_rows: int
) -> tuple[int, int]:
    """Flush one micro-batch past ``watermark``; returns (rows materialized, new watermark)."""
    events = await log.read(stream.name, watermark, batch_rows)
    if not events:
        return 0, watermark

    evolve = stream.on_schema_drift == "evolve"
    extras = evolved_columns(stream, [event.payload for event in events]) if evolve else {}

    schema = arrow_schema(stream)
    fields = list(schema)[:-2] + [pa.field(n, _ARROW_BY_SQL[t]) for n, t in extras.items()] + list(schema)[-2:]
    schema = pa.schema(fields)
    columns: dict[str, list[object]] = {field.name: [] for field in schema}
    for event in events:
        row = coerce_row(stream, event.payload, extras)
        for name in list(stream.schema) + list(extras):
            columns[name].append(row.get(name))
        columns["_offset"].append(event.offset)
        columns["_ingested_at"].append(event.ts.replace(tzinfo=None))
    batch = pa.table(columns, schema=schema)

    target = _sql(target_table(stream))
    for name, sql_type in extras.items():  # schema evolution: new fields become real columns
        column = exp.column(name).sql(dialect="duckdb", identify=True)
        await engine.execute_sql(f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS {column} {sql_type}")

    stage = TableRef(schema=_SCHEMA, name=f"_stage_{stream.name}")
    await engine.load(stage, batch.to_reader(), "create")
    last = events[-1].offset
    # BY NAME: an evolved batch has more columns than older target rows had
    insert = f"INSERT INTO {target} {'BY NAME ' if evolve else ''}SELECT * FROM {_sql(stage)}"
    # data + watermark move together: crash-safe exactly-once into the warehouse
    await engine.execute_all(
        [
            parse_one(insert, read="duckdb"),
            parse_one(f"DELETE FROM {_sql(_WATERMARKS)} WHERE stream = '{stream.name}'"),
            parse_one(f"INSERT INTO {_sql(_WATERMARKS)} VALUES ('{stream.name}', {last})"),
            parse_one(f"DROP TABLE {_sql(stage)}"),
        ]
    )
    return len(events), last


async def flush_streams(
    streams: Iterable[StreamDef], log: StreamLog, engine: EngineAdapter, *, batch_rows: int = 5000
) -> dict[str, int]:
    """Drain every stream into the warehouse; returns rows materialized per stream.

    Failures are isolated per stream: one stream whose batch cannot materialize
    (an uncoercible durable event, a dropped column) must not freeze every
    OTHER stream's watermark. The failing stream's error re-raises after the
    healthy ones flushed, so callers still see it.
    """
    flushed: dict[str, int] = {}
    first_error: Exception | None = None
    for stream in streams:
        try:
            count = await flush_stream(stream, log, engine, batch_rows=batch_rows)
        except Exception as exc:
            if first_error is None:
                first_error = exc
            continue
        if count:
            flushed[stream.name] = count
    if first_error is not None:
        raise first_error
    return flushed


async def sweep_streams(streams: Iterable[StreamDef], log: StreamLog, engine: EngineAdapter) -> dict[str, int]:
    """Apply retention: trim events that are both **materialized** (at or below the
    watermark) and **older** than the stream's declared retention. Unflushed events
    survive regardless of age; streams without a retention are never trimmed."""
    removed: dict[str, int] = {}
    expanded: list[StreamDef] = []
    for stream in streams:
        expanded.append(stream)
        if stream.on_schema_drift == "quarantine":
            expanded.append(quarantine_stream(stream))
    for stream in expanded:
        if stream.retention is None:
            continue
        window = parse_grain(stream.retention)
        watermark = await stream_watermark(stream, engine)
        if watermark == 0:
            continue
        count = await log.trim(stream.name, before_offset=watermark + 1, before_ts=datetime.now(UTC) - window)
        if count:
            removed[stream.name] = count
    return removed


def stream_consumers(compiled: CompiledProject, stream_name: str) -> set[str]:
    """Models whose SQL reads ``streams.<stream_name>`` — plus everything downstream
    of them, so a stream-triggered run refreshes the whole affected subgraph."""
    direct = {
        model.name
        for model in compiled.models.values()
        if model.ast is not None
        and any(t.db == _SCHEMA and t.name == stream_name for t in model.ast.find_all(exp.Table))
    }
    downstream: set[str] = set()
    for name in direct:
        downstream |= compiled.graph.descendants(name)
    return direct | downstream
