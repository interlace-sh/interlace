"""Query, engines, connections, schedules, and lineage."""

from __future__ import annotations

import asyncio
import contextlib

import pyarrow as pa
from litestar import get, post
from litestar.datastructures import State
from litestar.exceptions import ClientException
from litestar.params import FromQuery

from interlace.exceptions import QueryError
from interlace.graph.column_lineage import column_lineage
from interlace.graph.project import CompiledProject
from interlace.scheduler.daemon import (
    reload_if_stale,
)
from interlace.service.present import (
    _ast_projection_names,
    _jsonable,
    _output,
)
from interlace.service.types import (
    ConnectionInfo,
    EngineInfo,
    LineageModel,
    LineageResponse,
    LineageStream,
    QueryRequest,
    QueryResponse,
    ScheduleInfo,
)

_QUERY_MAX_CELL_BYTES = 8_000_000  # ~8 MB of rendered cells; the console inspects, never extracts


@post("/query", opt={"scope": "read"})
async def post_query(data: QueryRequest, state: State) -> QueryResponse:
    """Run a read-only query on the default engine. SELECT only, warehouse only —
    DDL/DML and external readers (files, HTTP) are rejected at parse; a row cap
    and a byte cap are always applied."""
    import time as _time

    from interlace.query import prepare_readonly

    try:
        bounded, limit = prepare_readonly(data.sql, state.engine.dialect, data.limit)
    except QueryError as exc:
        raise ClientException(detail=exc.message) from exc
    started = _time.perf_counter()

    async def _run() -> pa.Table:
        # prepare_readonly already fenced the query (SELECT-only, real tables not table
        # functions, no file paths), so no host file or network read can be expressed.
        reader = await state.engine.fetch_sandboxed(bounded)
        return await asyncio.to_thread(reader.read_all)

    try:
        table = await asyncio.wait_for(_run(), timeout=30.0)
    except TimeoutError as exc:
        interrupt = getattr(state.engine, "interrupt", None)
        if interrupt is not None:
            interrupt()  # free the engine thread; the cancelled fetch raises and is discarded
        raise ClientException(detail="query timed out after 30s — narrow it down") from exc
    except Exception as exc:  # engine errors (missing table, type errors) are the user's feedback
        raise ClientException(detail=str(exc)) from exc
    elapsed = (_time.perf_counter() - started) * 1000
    names = table.column_names
    records = table.to_pylist()
    truncated = len(records) > limit
    records = records[:limit]
    rows: list[list] = []
    budget = _QUERY_MAX_CELL_BYTES
    for record in records:
        row = [_jsonable(record[name]) for name in names]
        budget -= sum(len(cell) if isinstance(cell, str) else 16 for cell in row)
        if budget < 0:
            truncated = True
            break
        rows.append(row)
    return QueryResponse(
        columns=names,
        types=[str(field.type) for field in table.schema],
        rows=rows,
        row_count=len(rows),
        truncated=truncated,
        elapsed_ms=round(elapsed, 1),
    )


@get("/engines")
async def get_engines(state: State) -> list[EngineInfo]:
    from interlace.config.config import redact_dsn

    infos: list[EngineInfo] = []
    for name in sorted(state.engine_configs):
        cfg = state.engine_configs[name]
        infos.append(
            EngineInfo(
                name=name,
                type=cfg.type,
                dialect=cfg.resolved_dialect(),
                database=redact_dsn(cfg.database or ""),
                default=name == state.default_engine,
            )
        )
    return infos


@get("/connections")
async def get_connections(state: State) -> list[ConnectionInfo]:
    from interlace.connections import redacted

    infos: list[ConnectionInfo] = []
    for name in sorted(state.connections):
        public = redacted(name, state.connections[name])
        infos.append(ConnectionInfo(**public))
    return infos


@get("/schedules")
async def get_schedules(state: State) -> list[ScheduleInfo]:
    from datetime import datetime

    from cronsim import CronSim

    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    infos: list[ScheduleInfo] = []
    for name in sorted(compiled.models):
        schedule = compiled.models[name].schedule
        if not schedule:
            continue
        from interlace.scheduler.engine import schedule_kind

        kind, expression = schedule_kind(name, schedule)
        trigger_id = {"cron": f"cron:{name}", "every": f"interval:{name}", "watch": f"watch:{name}"}.get(kind)
        last = await state.store.get_trigger_last_fired(trigger_id) if trigger_id else None

        def _wire(moment: datetime | None) -> str | None:
            # trigger timestamps are naive LOCAL (the scheduler ticks datetime.now());
            # attach the local offset or every consumer reads them as UTC
            return moment.astimezone().isoformat() if moment else None

        next_fire: datetime | None = None
        if kind == "cron":
            with contextlib.suppress(Exception):
                next_fire = next(CronSim(expression, last or datetime.now()))
        elif kind == "every" and last is not None:
            from interlace.state.interval import parse_grain

            with contextlib.suppress(Exception):
                next_fire = last + parse_grain(expression)
        infos.append(
            ScheduleInfo(
                model=name,
                kind=kind,
                expression=expression,
                next_fire=_wire(next_fire),
                last_fired=_wire(last),
            )
        )
    return infos


@get("/lineage")
async def get_lineage(state: State, environment: FromQuery[str | None] = None) -> LineageResponse:
    """The whole graph in one payload: nodes (with warehouse-described column
    types), table edges, column lineage, and stream sources — the UI renders
    and traces without a request per node. ``?environment=`` inspects a sandbox."""
    from interlace.inspect import described_outputs

    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    types_by_model = await described_outputs(
        compiled, state.store, state.engines, environment or state.environment, cache=state.describe_cache
    )

    order = compiled.graph.topological_sort()
    types_by_model = {name: types_by_model.get(name, {}) for name in order}
    # Real warehouse columns feed column lineage: a Python model then contributes its
    # true output columns to the schema graph, so SQL models downstream of it qualify
    # and trace precisely (falling back to name-passthrough only where nothing's built).
    described = {name: list(types) for name, types in types_by_model.items() if types}
    lineage = column_lineage(compiled, known_columns=described)

    models: list[LineageModel] = []
    edges: list[list[str]] = []
    known = set(compiled.models)
    for name in order:
        model = compiled.models[name]
        types = types_by_model[name]
        columns = list(types) or list(lineage.get(name, {})) or _ast_projection_names(model)
        models.append(
            LineageModel(
                name=name,
                output=_output(model),
                strategy=model.strategy,
                engine=model.engine,
                tags=list(model.tags),
                columns=columns,
                types=types,
                has_schedule=bool(model.schedule),
                has_checks=bool(model.checks) or bool(compiled.python_checks.get(name)),
            )
        )
        edges.extend([dep, name] for dep in model.dependencies)

    from sqlglot import exp as _exp

    streams: list[LineageStream] = []
    stream_keys = {f"streams.{stream.name}" for stream in state.streams.values()}
    for stream in state.streams.values():
        consumers = sorted(
            model.name
            for model in compiled.models.values()
            if model.ast is not None
            and any(t.db == "streams" and t.name == stream.name for t in model.ast.find_all(_exp.Table))
        )
        key = f"streams.{stream.name}"
        streams.append(
            LineageStream(
                name=key,
                stream=stream.name,
                columns=list(stream.schema),
                types=dict(stream.schema),
                consumers=consumers,
            )
        )
        edges.extend([key, consumer] for consumer in consumers)

    # column sources: keep only references that resolve to a node on the canvas
    # (a VALUES alias like `t` is a phantom, not a navigable source). column_lineage
    # already traces through opaque models (Python / *) by name-passthrough, so no
    # separate fallback here.
    column_sources = {
        name: {col: [[t, c] for t, c in refs if t in known or t in stream_keys] for col, refs in sources.items()}
        for name, sources in lineage.items()
        if sources
    }
    return LineageResponse(models=models, edges=edges, columns=column_sources, streams=streams)
