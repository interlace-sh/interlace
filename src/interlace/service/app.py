"""The interlace HTTP API (Litestar).

A read + trigger surface over a project: list/inspect models, preview a plan
(with per-change SQL + impacted columns), apply it (build changed snapshots and
promote — guarded against breaking changes unless forced), inspect environments,
list/inspect runs, and enqueue runs onto the durable queue (a running ``interlace
scheduler`` drains them). The project is loaded and compiled once at startup and
held on app state; the warehouse engine and control-plane store are opened for
the app's lifetime. msgspec structs are the wire types (Litestar serializes them
natively). Scoped API-key auth is enforced once a key exists (see auth.py), and
OpenAPI docs render via Scalar at ``/schema/scalar``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import msgspec
from litestar import Litestar, Request, get, post
from litestar.datastructures import State
from litestar.exceptions import ClientException, ImproperlyConfiguredException, NotFoundException
from litestar.openapi import OpenAPIConfig
from litestar.openapi.plugins import ScalarRenderPlugin
from litestar.params import FromPath, FromQuery
from litestar.response import ServerSentEvent, ServerSentEventMessage

from interlace import __version__
from interlace.dsl.decorators import StreamDef
from interlace.exceptions import CheckError, SelectionError, StreamError
from interlace.graph.column_lineage import column_lineage
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.graph.selectors import select_models
from interlace.plan.apply import apply as apply_plan
from interlace.plan.differ import diff
from interlace.project import Project
from interlace.service.auth import auth_guard
from interlace.state.snapshot import ChangeCategory
from interlace.streaming.log import Event
from interlace.streaming.materializer import (
    ensure_stream_tables,
    flush_stream,
    flush_streams,
    stream_consumers,
    stream_watermark,
)
from interlace.streaming.schema import validate_rows


class ModelInfo(msgspec.Struct):
    name: str
    output: str  # back-compat: "sink" or the materialisation value
    materialise: str
    strategy: str
    is_sink: bool
    fingerprint: str
    depends_on: list[str]
    tags: list[str]
    owner: str | None
    schedule: dict[str, str] | None


class ModelDetail(msgspec.Struct):
    name: str
    output: str
    materialise: str
    strategy: str
    is_sink: bool
    fingerprint: str
    depends_on: list[str]
    upstream: list[str]
    downstream: list[str]
    columns: dict[str, list[str]]
    tags: list[str]
    owner: str | None
    schedule: dict[str, str] | None


class Change(msgspec.Struct):
    name: str
    change_type: str
    category: str | None
    previous_fingerprint: str | None = None
    new_fingerprint: str | None = None
    impacted_columns: list[str] = msgspec.field(default_factory=list)
    new_sql: str | None = None
    previous_sql: str | None = None
    reused: bool = False  # output provably identical: recorded without a rebuild


class PlanResponse(msgspec.Struct):
    environment: str
    changes: list[Change]


class RunInfo(msgspec.Struct):
    id: int
    flow_selector: list[str]
    state: str
    attempts: int
    error: str | None
    enqueued_at: str | None = None
    priority: int = 0
    partition: list[str] | None = None


class CreateRun(msgspec.Struct):
    selectors: list[str] = msgspec.field(default_factory=list)
    environment: str | None = None


class CreateRunResult(msgspec.Struct):
    enqueued: int
    models: list[str]


class EventInfo(msgspec.Struct):
    seq: int
    ts: str
    type: str
    entity: str | None
    payload: dict | None


class RunDetail(msgspec.Struct):
    id: int
    flow_selector: list[str]
    state: str
    attempts: int
    error: str | None
    enqueued_at: str | None
    priority: int
    partition: list[str] | None
    events: list[EventInfo]


class EnvironmentInfo(msgspec.Struct):
    name: str
    models: int
    changed: int  # compiled models whose fingerprint differs from the one promoted here


class ApplyRequest(msgspec.Struct):
    selectors: list[str] = msgspec.field(default_factory=list)
    environment: str | None = None
    force: bool = False  # required to proceed when the plan has breaking changes


class ApplyResponse(msgspec.Struct):
    environment: str
    built: list[str]
    promoted: int
    breaking: bool
    reused: list[str] = msgspec.field(default_factory=list)


class CheckResultInfo(msgspec.Struct):
    id: int
    environment: str
    model: str
    fingerprint: str
    check_name: str
    check_type: str
    severity: str
    status: str
    failures: int
    message: str | None
    executed_at: str


class StreamInfo(msgspec.Struct):
    name: str
    schema: dict[str, str]
    table: str
    head: int  # highest offset accepted into the log
    watermark: int  # highest offset materialized into the warehouse


class StreamDetail(msgspec.Struct):
    name: str
    schema: dict[str, str]
    table: str
    head: int
    watermark: int
    idempotency_key: str | None
    recent: list[dict]  # latest payloads, newest last


class PublishResult(msgspec.Struct):
    accepted: int
    deduplicated: int
    last_offset: int | None
    materialized: int  # rows flushed to the warehouse in this request


class GcRequest(msgspec.Struct):
    grace: str = "7d"  # keep unreferenced snapshots younger than this
    dry_run: bool = False


class GcResponse(msgspec.Struct):
    removed_snapshots: int
    dropped_tables: list[str]
    kept_snapshots: int
    dry_run: bool


def _output(model: CompiledModel) -> str:
    return "sink" if model.export is not None else model.materialise


def _info(model: CompiledModel) -> ModelInfo:
    return ModelInfo(
        name=model.name,
        output=_output(model),
        materialise=model.materialise,
        strategy=model.strategy,
        is_sink=model.export is not None,
        fingerprint=model.fingerprint,
        depends_on=list(model.dependencies),
        tags=list(model.tags),
        owner=model.owner,
        schedule=model.schedule,
    )


@get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@get("/models")
async def get_models(state: State) -> list[ModelInfo]:
    compiled: CompiledProject = state.compiled
    return [_info(compiled.models[name]) for name in compiled.graph.topological_sort()]


@get("/models/{name:str}")
async def get_model(name: FromPath[str], state: State) -> ModelDetail:
    compiled: CompiledProject = state.compiled
    if name not in compiled.models:
        raise NotFoundException(detail=f"unknown model: {name}")
    model = compiled.models[name]
    cols = column_lineage(compiled).get(name, {})
    return ModelDetail(
        name=name,
        output=_output(model),
        materialise=model.materialise,
        strategy=model.strategy,
        is_sink=model.export is not None,
        fingerprint=model.fingerprint,
        depends_on=list(model.dependencies),
        upstream=sorted(compiled.graph.ancestors(name)),
        downstream=sorted(compiled.graph.descendants(name)),
        columns={col: [f"{t}.{c}" for t, c in refs] for col, refs in cols.items()},
        tags=list(model.tags),
        owner=model.owner,
        schedule=model.schedule,
    )


@get("/plan")
async def get_plan(state: State, environment: FromQuery[str | None] = None) -> PlanResponse:
    env = environment or state.environment
    compiled: CompiledProject = state.compiled
    plan = await diff(compiled, env, state.store)
    reused = {snapshot.name for snapshot in plan.reuses}
    changes: list[Change] = []
    for change in plan.changes:
        previous_sql: str | None = None
        if change.previous_fingerprint is not None:
            snapshot = await state.store.get_snapshot(change.name, change.previous_fingerprint)
            previous_sql = snapshot.definition_sql if snapshot else None
        model = compiled.models.get(change.name)
        changes.append(
            Change(
                name=change.name,
                change_type=change.change_type.value,
                category=change.category.value if change.category else None,
                previous_fingerprint=change.previous_fingerprint,
                new_fingerprint=change.new_fingerprint,
                impacted_columns=list(change.impacted_columns),
                new_sql=model.definition_sql if model else None,
                previous_sql=previous_sql,
                reused=change.name in reused,
            )
        )
    return PlanResponse(environment=env, changes=changes)


@get("/environments")
async def get_environments(state: State) -> list[EnvironmentInfo]:
    compiled: CompiledProject = state.compiled
    out: list[EnvironmentInfo] = []
    for env in await state.store.list_environments():
        promoted = await state.store.get_environment(env)
        changed = sum(1 for model in compiled.models.values() if promoted.get(model.name) != model.fingerprint)
        out.append(EnvironmentInfo(name=env, models=len(promoted), changed=changed))
    return out


@get("/runs")
async def get_runs(state: State) -> list[RunInfo]:
    runs: list[RunInfo] = []
    for run in await state.store.list_runs():
        partition = [str(run["partition_start"]), str(run["partition_end"])] if run["partition_start"] else None
        runs.append(
            RunInfo(
                id=run["id"],
                flow_selector=run["flow_selector"],
                state=run["state"],
                attempts=run["attempts"],
                error=run["error"],
                enqueued_at=run["enqueued_at"],
                priority=run["priority"],
                partition=partition,
            )
        )
    return runs


@get("/runs/{run_id:int}")
async def get_run(run_id: FromPath[int], state: State) -> RunDetail:
    run = await state.store.get_run(run_id)
    if run is None:
        raise NotFoundException(detail=f"unknown run: {run_id}")
    # lifecycle events are keyed by run id (worker) and idempotency key (enqueue)
    events = await state.store.events_for_entity(str(run_id))
    if run["idempotency_key"]:
        events = sorted(events + await state.store.events_for_entity(run["idempotency_key"]), key=lambda e: e["seq"])
    partition = [str(run["partition_start"]), str(run["partition_end"])] if run["partition_start"] else None
    return RunDetail(
        id=run["id"],
        flow_selector=run["flow_selector"],
        state=run["state"],
        attempts=run["attempts"],
        error=run["error"],
        enqueued_at=run["enqueued_at"],
        priority=run["priority"],
        partition=partition,
        events=[EventInfo(**event) for event in events],
    )


@post("/runs", opt={"scope": "write"})
async def create_run(data: CreateRun, state: State) -> CreateRunResult:
    compiled: CompiledProject = state.compiled
    env = data.environment or state.environment
    try:
        selected = select_models(data.selectors, compiled) if data.selectors else set(compiled.models)
    except SelectionError as exc:
        raise ClientException(detail=exc.message) from exc
    models = sorted(selected)
    key = f"api:{env}:{uuid4().hex}"
    enqueued = await state.store.enqueue_run(key, models, None, 0)
    if enqueued:
        await state.store.append_event("run.enqueued", entity=key, payload={"models": models})
    return CreateRunResult(enqueued=1 if enqueued else 0, models=models)


@post("/apply", opt={"scope": "write"})
async def post_apply(data: ApplyRequest, state: State) -> ApplyResponse:
    compiled: CompiledProject = state.compiled
    env = data.environment or state.environment
    try:
        selected = select_models(data.selectors, compiled) if data.selectors else None
    except SelectionError as exc:
        raise ClientException(detail=exc.message) from exc
    async with state.apply_lock:
        plan = await diff(compiled, env, state.store, select=selected)
        breaking = plan.has_breaking_changes
        if breaking and not data.force:
            names = ", ".join(c.name for c in plan.changes if c.category is ChangeCategory.BREAKING)
            raise ClientException(detail=f"plan has breaking changes ({names}); resubmit with force=true")
        if plan.is_empty:
            return ApplyResponse(environment=env, built=[], promoted=0, breaking=False)
        await state.store.append_event("apply.started", entity=env, payload={"models": plan.promote})
        try:
            result = await apply_plan(
                plan, compiled=compiled, engine=state.engine, state=state.store, base_path=state.root
            )
        except CheckError as exc:
            await state.store.append_event("apply.blocked", entity=env, payload={"reason": exc.message})
            raise ClientException(detail=exc.message) from exc
        await state.store.append_event(
            "apply.finished", entity=env, payload={"built": result.built, "promoted": result.promoted}
        )
    return ApplyResponse(
        environment=env, built=result.built, promoted=result.promoted, breaking=breaking, reused=result.reused
    )


@get("/checks")
async def get_checks(state: State, model: FromQuery[str | None] = None) -> list[CheckResultInfo]:
    return [CheckResultInfo(**row) for row in await state.store.list_check_results(model)]


async def _enqueue_stream_consumers(state: State, stream: StreamDef) -> None:
    """A flush advanced the stream table: enqueue the models that read it.

    The idempotency key carries the watermark, so repeated flushes at the same
    position debounce into one run while new data keeps enqueuing new runs.
    """
    consumers = stream_consumers(state.compiled, stream.name)
    if not consumers:
        return
    watermark = await stream_watermark(stream, state.engine)
    key = f"stream:{stream.name}:{watermark}"
    if await state.store.enqueue_run(key, sorted(consumers), None, 0):
        await state.store.append_event("run.enqueued", entity=key, payload={"models": sorted(consumers)})


def _stream_or_404(state: State, name: str) -> StreamDef:
    stream: StreamDef | None = state.streams.get(name)
    if stream is None:
        raise NotFoundException(detail=f"unknown stream: {name}")
    return stream


@get("/streams")
async def get_streams(state: State) -> list[StreamInfo]:
    out = []
    for stream in state.streams.values():
        out.append(
            StreamInfo(
                name=stream.name,
                schema=stream.schema,
                table=f"streams.{stream.name}",
                head=await state.stream_log.head(stream.name),
                watermark=await stream_watermark(stream, state.engine),
            )
        )
    return out


@get("/streams/{name:str}")
async def get_stream(name: FromPath[str], state: State) -> StreamDetail:
    stream = _stream_or_404(state, name)
    head = await state.stream_log.head(name)
    events = await state.stream_log.read(name, max(0, head - 20), 20)
    return StreamDetail(
        name=stream.name,
        schema=stream.schema,
        table=f"streams.{stream.name}",
        head=head,
        watermark=await stream_watermark(stream, state.engine),
        idempotency_key=stream.idempotency_key,
        recent=[dict(event.payload, _offset=event.offset) for event in events],
    )


@post("/streams/{name:str}", opt={"scope": "write"})
async def publish(name: FromPath[str], data: dict | list, state: State) -> PublishResult:
    """Publish one event (object) or a batch (array). Durable before this returns."""
    stream = _stream_or_404(state, name)
    rows = data if isinstance(data, list) else [data]
    try:
        validate_rows(stream, rows)
    except StreamError as exc:
        raise ClientException(detail=exc.message) from exc
    events = [
        Event(payload=row, idempotency_key=str(row[stream.idempotency_key]) if stream.idempotency_key else None)
        for row in rows
    ]
    result = await state.stream_log.append(name, events)
    accepted = result.deduped.count(False)
    async with state.apply_lock:  # micro-batch straight through: POST -> queryable
        materialized = await flush_stream(stream, state.stream_log, state.engine)
    if materialized:
        await state.store.append_event("stream.flushed", entity=name, payload={"rows": materialized})
        await _enqueue_stream_consumers(state, stream)
    return PublishResult(
        accepted=accepted,
        deduplicated=result.deduped.count(True),
        last_offset=max(result.offsets) if result.offsets else None,
        materialized=materialized,
    )


@post("/gc", opt={"scope": "admin"})
async def post_gc(state: State, data: GcRequest | None = None) -> GcResponse:
    """Garbage-collect unreferenced snapshots and their physical tables."""
    from interlace.state.interval import parse_grain
    from interlace.state.janitor import gc as run_gc

    request = data or GcRequest()
    try:
        grace = parse_grain(request.grace)
    except ValueError as exc:
        raise ClientException(detail=str(exc)) from exc
    async with state.apply_lock:
        result = await run_gc(state.store, state.engine, grace=grace, dry_run=request.dry_run)
    if result.removed_snapshots and not request.dry_run:
        await state.store.append_event(
            "gc.finished",
            payload={"snapshots": len(result.removed_snapshots), "tables": result.dropped_tables},
        )
    return GcResponse(
        removed_snapshots=len(result.removed_snapshots),
        dropped_tables=result.dropped_tables,
        kept_snapshots=result.kept_snapshots,
        dry_run=request.dry_run,
    )


@get("/events")
async def get_events(state: State, after: FromQuery[int] = 0) -> list[EventInfo]:
    return [EventInfo(**event) for event in await state.store.read_events(after)]


@get("/events/stream")
async def stream_events(state: State, request: Request) -> ServerSentEvent:
    after = int(request.headers.get("Last-Event-ID") or 0)

    async def tail() -> AsyncIterator[ServerSentEventMessage]:
        cursor = after
        while True:
            for event in await state.store.read_events(cursor):
                cursor = int(event["seq"])
                yield ServerSentEventMessage(data=json.dumps(event), event=str(event["type"]), id=str(cursor))
            await asyncio.sleep(0.5)

    return ServerSentEvent(tail())


def create_app(
    root: Path | str,
    environment: str = "dev",
    quack: str | None = None,
    quack_token: str | None = None,
    scheduler: bool = False,
    scheduler_interval: float = 60.0,
) -> Litestar:
    """Build the Litestar app for the project at ``root``.

    ``scheduler=True`` makes this the combined daemon: the HTTP API plus a
    background scheduler loop (tick triggers, drain the run queue) in one
    process — the default for ``interlace serve``. ``quack`` (a
    ``quack:<host>:<port>`` URI) additionally serves the warehouse over the
    quack protocol so other processes — CLI runs, ad-hoc DuckDB clients —
    share this process's warehouse concurrently.
    """

    @asynccontextmanager
    async def lifespan(app: Litestar) -> AsyncIterator[None]:
        from datetime import datetime

        from interlace.scheduler.engine import TriggerEngine, build_triggers
        from interlace.scheduler.worker import drain

        project = Project.load(root)
        store = await project.open_state()
        engine = project.open_engine()
        if quack:
            from interlace.engines.quack import QuackAdapter, sql_literal

            if isinstance(engine, QuackAdapter):
                raise ImproperlyConfiguredException(detail="cannot re-serve a quack-connected warehouse")
            token_sql = f", token := {sql_literal(quack_token)}" if quack_token else ""
            await engine.execute_sql(f"CALL quack_serve({sql_literal(quack)}{token_sql})")
        compiled = project.compile()
        stream_log = await project.open_stream_log()
        streams = {stream.name: stream for stream in project.streams}
        if streams:
            await ensure_stream_tables(streams.values(), engine)
        app.state.compiled = compiled
        app.state.store = store
        app.state.engine = engine
        app.state.environment = environment
        app.state.root = project.root
        app.state.apply_lock = asyncio.Lock()  # serialise applies against the single warehouse connection
        app.state.streams = streams
        app.state.stream_log = stream_log

        async def scheduler_loop() -> None:
            trigger_engine = TriggerEngine(build_triggers(compiled), store)
            while True:
                await trigger_engine.tick(datetime.now())
                await drain(store, compiled, engine, environment, base_path=project.root)
                if streams:  # catch up anything the publish path didn't flush
                    async with app.state.apply_lock:
                        flushed = await flush_streams(streams.values(), stream_log, engine)
                    for stream_name in flushed:
                        await _enqueue_stream_consumers(app.state, streams[stream_name])
                await asyncio.sleep(scheduler_interval)

        loop_task = asyncio.create_task(scheduler_loop()) if scheduler else None
        try:
            yield
        finally:
            if loop_task is not None:
                loop_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await loop_task
            await stream_log.close()
            await store.close()
            engine.close()

    return Litestar(
        route_handlers=[
            health,
            get_models,
            get_model,
            get_plan,
            get_environments,
            get_runs,
            get_run,
            create_run,
            post_apply,
            get_checks,
            get_streams,
            get_stream,
            publish,
            post_gc,
            get_events,
            stream_events,
        ],
        lifespan=[lifespan],
        guards=[auth_guard],
        openapi_config=OpenAPIConfig(
            title="interlace",
            version=__version__,
            description="Python/SQL-first data platform: transformation, orchestration, and streaming.",
            render_plugins=[ScalarRenderPlugin()],
        ),
    )
