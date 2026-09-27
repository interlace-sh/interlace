"""The stream materializer — micro-batches from the log into the warehouse.

A flush drains everything past the stream's watermark in ``batch_rows`` chunks;
each chunk stages one Arrow batch and moves ``stage -> target table + watermark``
(plus any evolve ``ALTER``s) in a single engine transaction. Crash anywhere leaves
either the old watermark (events re-read, stage overwritten — no duplicates) or
the new one — **exactly-once landing into the warehouse** given a transactional
``execute_all`` (DuckDB / ADBC), without coordinating with the log. The watermark
lives in the warehouse (``streams._watermarks``) precisely so it commits atomically
with the data; the log's consumer-group lease/commit machinery is for external
consumers, not this path.

Stream tables land in the ``streams`` schema (``streams.<name>``) with the
declared fields plus ``_offset`` and ``_ingested_at``, so SQL models simply
``FROM streams.<name>``.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime

import pyarrow as pa
from sqlglot import exp

from interlace.dsl.decorators import StreamDef
from interlace.engines.base import EngineAdapter
from interlace.exceptions import ConfigurationError
from interlace.graph.project import CompiledProject
from interlace.ir.relation import TableRef, drop
from interlace.state.interval import parse_grain
from interlace.streaming.log import StreamLog
from interlace.streaming.schema import arrow_schema, coerce_row, evolved_columns, sql_columns

_SCHEMA = "streams"
_WATERMARKS = TableRef(schema=_SCHEMA, name="_watermarks")


def target_table(stream: StreamDef) -> TableRef:
    return TableRef(schema=_SCHEMA, name=stream.name)


def _ident(name: str) -> exp.Expression:
    """Quote every name. Stream columns are caller-chosen and include reserved words (`at`)."""
    return exp.to_identifier(name, quoted=True)


def _column_def(name: str, sql_type: str, *, exists: bool = False) -> exp.ColumnDef:
    return exp.ColumnDef(this=_ident(name), kind=exp.DataType.build(sql_type), exists=exists)


def _create_table(table: TableRef, columns: list[tuple[str, str]]) -> exp.Create:
    schema = exp.Schema(
        this=table.to_expr(),
        expressions=[_column_def(name, sql_type) for name, sql_type in columns],
    )
    return exp.Create(this=schema, kind="TABLE", exists=True)


async def ensure_stream_tables(streams: Iterable[StreamDef], engine: EngineAdapter) -> None:
    """Create the streams schema, watermark table, and one table per stream."""
    await engine.create_schema(_SCHEMA)
    await engine.execute(_create_table(_WATERMARKS, [("stream", "TEXT"), ("committed_offset", "BIGINT")]))
    for stream in streams:
        await engine.execute(_create_table(target_table(stream), list(sql_columns(stream))))


async def stream_watermark(stream: StreamDef, engine: EngineAdapter) -> int:
    reader = await engine.fetch(
        exp.select(exp.alias_(exp.Max(this=exp.column("committed_offset")), "offset"))
        .from_(_WATERMARKS.to_expr())
        .where(exp.column("stream").eq(exp.Literal.string(stream.name)))
    )
    rows = reader.read_all().to_pylist()
    return int(rows[0]["offset"] or 0) if rows else 0


_ARROW_BY_SQL = {"BIGINT": pa.int64(), "DOUBLE": pa.float64(), "BOOLEAN": pa.bool_(), "TEXT": pa.string()}


def quarantine_stream(stream: StreamDef) -> StreamDef:
    """The shadow stream that receives a quarantine-mode stream's failing events."""
    return StreamDef(
        name=f"{stream.name}__quarantine", schema={"error": "text", "payload": "json"}, retention=stream.retention
    )


def _require_txn_engine(engine: EngineAdapter) -> None:
    if not engine.caps.supports_transactions:
        raise ConfigurationError(
            "stream materialisation requires transactional execute_all "
            f"(engine dialect={engine.dialect!r} does not support it — use DuckDB/Postgres, not Spark)",
            details={"dialect": engine.dialect},
        )


async def flush_stream(stream: StreamDef, log: StreamLog, engine: EngineAdapter, *, batch_rows: int = 5000) -> int:
    """Drain everything durable for ``stream`` into the warehouse in
    ``batch_rows`` micro-batches; returns rows materialized. Draining (not a
    single batch) is what lets callers — the flusher, an apply's pre-flush,
    shutdown — assume the warehouse has caught up with the log when this
    returns."""
    _require_txn_engine(engine)
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

    target = target_table(stream).to_expr()
    # Evolve ALTERs land in the same execute_all txn as insert + watermark so a
    # crash cannot leave a widened schema with a stale watermark mid-flush.
    alters = [
        exp.Alter(
            this=target.copy(),
            kind="TABLE",
            actions=[_column_def(name, sql_type, exists=True)],
        )
        for name, sql_type in extras.items()
    ]

    stage = TableRef(schema=_SCHEMA, name=f"_stage_{stream.name}")
    await engine.load(stage, batch.to_reader(), "create")
    last = events[-1].offset
    # Name every column. An evolved batch has more columns than older rows had;
    # a positional INSERT would land them in the wrong place, and BY NAME is DuckDB-only.
    names = [field.name for field in schema]
    insert = exp.Insert(
        this=exp.Schema(this=target.copy(), expressions=[_ident(name) for name in names]),
        expression=exp.select(*(exp.column(name, quoted=True) for name in names)).from_(stage.to_expr()),
    )
    stream_lit = exp.Literal.string(stream.name)
    # data + watermark (+ evolve DDL) move together: crash-safe exactly-once landing
    await engine.execute_all(
        [
            *alters,
            insert,
            exp.Delete(this=_WATERMARKS.to_expr(), where=exp.Where(this=exp.column("stream").eq(stream_lit))),
            exp.Insert(
                this=_WATERMARKS.to_expr(),
                expression=exp.Values(
                    expressions=[exp.Tuple(expressions=[stream_lit.copy(), exp.Literal.number(last)])]
                ),
            ),
            drop(stage.to_expr(), kind="TABLE"),
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
