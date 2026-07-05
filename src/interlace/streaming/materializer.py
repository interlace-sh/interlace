"""The stream materializer — micro-batches from the log into the warehouse.

Each flush reads events past the stream's watermark, converts them to one Arrow
batch, stages it, and moves ``stage -> target table + watermark`` in a single
engine transaction. Crash anywhere leaves either the old watermark (events
re-read, stage overwritten — no duplicates) or the new one — **exactly-once
into the warehouse** without coordinating with the log. The watermark lives in
the warehouse (``streams._watermarks``) precisely so it commits atomically with
the data; the log's consumer-group lease/commit machinery is for external
consumers, not this path.

Stream tables land in the ``streams`` schema (``streams.<name>``) with the
declared fields plus ``_offset`` and ``_ingested_at``, so SQL models simply
``FROM streams.<name>``.
"""

from __future__ import annotations

from collections.abc import Iterable

import pyarrow as pa
from sqlglot import exp, parse_one

from interlace.dsl.decorators import StreamDef
from interlace.engines.base import EngineAdapter
from interlace.ir.relation import TableRef
from interlace.streaming.log import StreamLog
from interlace.streaming.schema import arrow_schema, coerce_value, sql_columns

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


async def flush_stream(stream: StreamDef, log: StreamLog, engine: EngineAdapter, *, batch_rows: int = 5000) -> int:
    """Flush one micro-batch for ``stream``; returns rows materialized."""
    watermark = await stream_watermark(stream, engine)
    events = await log.read(stream.name, watermark, batch_rows)
    if not events:
        return 0

    schema = arrow_schema(stream)
    columns: dict[str, list[object]] = {field.name: [] for field in schema}
    for event in events:
        for name, type_name in stream.schema.items():
            columns[name].append(coerce_value(type_name, event.payload.get(name)))
        columns["_offset"].append(event.offset)
        columns["_ingested_at"].append(event.ts.replace(tzinfo=None))
    batch = pa.table(columns, schema=schema)

    stage = TableRef(schema=_SCHEMA, name=f"_stage_{stream.name}")
    await engine.load(stage, batch.to_reader(), "create")
    last = events[-1].offset
    # data + watermark move together: crash-safe exactly-once into the warehouse
    await engine.execute_all(
        [
            parse_one(f"INSERT INTO {_sql(target_table(stream))} SELECT * FROM {_sql(stage)}"),
            parse_one(f"DELETE FROM {_sql(_WATERMARKS)} WHERE stream = '{stream.name}'"),
            parse_one(f"INSERT INTO {_sql(_WATERMARKS)} VALUES ('{stream.name}', {last})"),
            parse_one(f"DROP TABLE {_sql(stage)}"),
        ]
    )
    return len(events)


async def flush_streams(
    streams: Iterable[StreamDef], log: StreamLog, engine: EngineAdapter, *, batch_rows: int = 5000
) -> dict[str, int]:
    """Flush every stream once; returns rows materialized per stream."""
    flushed: dict[str, int] = {}
    for stream in streams:
        count = await flush_stream(stream, log, engine, batch_rows=batch_rows)
        if count:
            flushed[stream.name] = count
    return flushed
