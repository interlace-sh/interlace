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
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import msgspec
from litestar import Litestar, Request, delete, get, post
from litestar.datastructures import State
from litestar.exceptions import ClientException, ImproperlyConfiguredException, NotFoundException
from litestar.openapi import OpenAPIConfig
from litestar.openapi.plugins import ScalarRenderPlugin
from litestar.params import FromPath, FromQuery
from litestar.response import Redirect, ServerSentEvent, ServerSentEventMessage
from litestar.static_files import create_static_files_router

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
    flush_streams,
    quarantine_stream,
    stream_consumers,
    stream_watermark,
    sweep_streams,
)
from interlace.streaming.schema import partition_rows, validate_rows, validate_rows_evolve


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
    sql: str | None = None  # canonical SQL; None for Python models


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
    transfers: list[str] = msgspec.field(default_factory=list)  # explicit cross-engine movement


class RunInfo(msgspec.Struct):
    id: int
    flow_selector: list[str]
    state: str
    attempts: int
    error: str | None
    enqueued_at: str | None = None
    priority: int = 0
    partition: list[str] | None = None
    restate: bool = False
    # how the run came to be — the enqueue key's prefix names the trigger
    # (cron: / interval: / api: / stream:)
    idempotency_key: str | None = None


class CreateRun(msgspec.Struct):
    selectors: list[str] = msgspec.field(default_factory=list)
    environment: str | None = None
    start: str | None = None  # ISO timestamp: backfill window start (incremental models)
    end: str | None = None  # ISO timestamp: backfill window end
    restate: bool = False  # reprocess the window instead of skipping filled intervals


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
    restate: bool = False
    idempotency_key: str | None = None


class EnvironmentInfo(msgspec.Struct):
    name: str
    models: int
    changed: int  # compiled models whose fingerprint differs from the one promoted here
    promoted_at: str | None = None  # when the environment last moved


class ApplyRequest(msgspec.Struct):
    selectors: list[str] = msgspec.field(default_factory=list)
    environment: str | None = None
    force: bool = False  # required to proceed when the plan has breaking changes
    forward_only: bool = False  # history-keeping models inherit their table; new logic applies ahead


class ApplyResponse(msgspec.Struct):
    environment: str
    built: list[str]
    promoted: int
    breaking: bool
    reused: list[str] = msgspec.field(default_factory=list)
    transfers: list[str] = msgspec.field(default_factory=list)
    # per-model row movement (inserted/updated/deleted) and build seconds
    rows: dict[str, dict[str, int]] = msgspec.field(default_factory=dict)
    timings: dict[str, float] = msgspec.field(default_factory=dict)


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
    on_schema_drift: str = "reject"


class StreamDetail(msgspec.Struct):
    name: str
    schema: dict[str, str]
    table: str
    head: int
    watermark: int
    idempotency_key: str | None
    recent: list[dict]  # latest payloads, newest last


class PublishResult(msgspec.Struct):
    """Ack for a durable append. Materialization is micro-batched: a flusher task
    coalesces publishes into one warehouse write moments later — poll the stream's
    ``watermark`` (GET /streams/{name}) to observe it land."""

    accepted: int
    deduplicated: int
    last_offset: int | None
    quarantined: int = 0  # events diverted to <stream>__quarantine (quarantine mode)


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
    from interlace import __version__

    return {"status": "ok", "version": __version__}


@get("/", include_in_schema=False)
async def ui_redirect() -> Redirect:
    return Redirect(path="/ui/")


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
    cols = state.lineage.get(name, {})
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
        sql=model.definition_sql,
    )


@get("/plan")
async def get_plan(state: State, environment: FromQuery[str | None] = None) -> PlanResponse:
    env = environment or state.environment
    compiled: CompiledProject = state.compiled
    plan = await diff(compiled, env, state.store)
    reused = {snapshot.name for snapshot in plan.reuses}
    previous_snapshots = await state.store.get_snapshots(
        (c.name, c.previous_fingerprint) for c in plan.changes if c.previous_fingerprint is not None
    )
    changes: list[Change] = []
    for change in plan.changes:
        previous_sql: str | None = None
        if change.previous_fingerprint is not None:
            snapshot = previous_snapshots.get((change.name, change.previous_fingerprint))
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
    return PlanResponse(
        environment=env,
        changes=changes,
        transfers=[f"{t.model}: {t.source.name} -> {t.target.name} ({t.via})" for t in plan.transfers],
    )


@get("/environments")
async def get_environments(state: State) -> list[EnvironmentInfo]:
    compiled: CompiledProject = state.compiled
    out: list[EnvironmentInfo] = []
    promoted_ats = await state.store.environment_promoted_at()
    for env in await state.store.list_environments():
        promoted = await state.store.get_environment(env)
        changed = sum(1 for model in compiled.models.values() if promoted.get(model.name) != model.fingerprint)
        out.append(EnvironmentInfo(name=env, models=len(promoted), changed=changed, promoted_at=promoted_ats.get(env)))
    return out


@delete("/environments/{name:str}", opt={"scope": "admin"}, status_code=200)
async def drop_environment_endpoint(name: FromPath[str], state: State, force: FromQuery[bool] = False) -> dict:
    """Drop an environment: views removed, snapshots released to gc. Production needs force=true."""
    from interlace.plan.plan import PRODUCTION_ENV
    from interlace.state.janitor import drop_environment

    if name == PRODUCTION_ENV and not force:
        raise ClientException(detail=f"{name!r} is the production environment; pass force=true to drop it")
    if not await state.store.get_environment(name):
        raise NotFoundException(detail=f"unknown environment: {name}")
    async with state.apply_lock:
        dropped = await drop_environment(state.store, engines=state.engines, environment=name)
    await state.store.append_event("environment.dropped", entity=name, payload={"views": dropped})
    return {"environment": name, "dropped_views": dropped}


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
                restate=run["restate"],
                idempotency_key=run["idempotency_key"],
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
        restate=run["restate"],
        idempotency_key=run["idempotency_key"],
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
    partition = None
    if data.start or data.end:
        from datetime import datetime

        def naive(value: str) -> str:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is not None:  # ledger timestamps are naive local
                parsed = parsed.astimezone().replace(tzinfo=None)
            return parsed.isoformat()

        try:
            bounds = tuple(naive(v) if v else "" for v in (data.start, data.end))
        except ValueError as exc:
            raise ClientException(detail=f"start/end must be ISO timestamps: {exc}") from exc
        partition = (bounds[0] or None, bounds[1] or None)
    key = f"api:{env}:{uuid4().hex}"
    enqueued = await state.store.enqueue_run(key, models, partition, 0, restate=data.restate)
    if enqueued:
        await state.store.append_event("run.enqueued", entity=key, payload={"models": models})
    return CreateRunResult(enqueued=1 if enqueued else 0, models=models)


@post("/runs/{run_id:int}/cancel", opt={"scope": "write"}, status_code=200)
async def cancel_run(run_id: FromPath[int], state: State) -> dict:
    """Cancel a run: queued cancels immediately; running cancels cooperatively
    at the worker's next heartbeat."""
    outcome = await state.store.request_cancel(run_id)
    if outcome is None:
        raise NotFoundException(detail=f"run {run_id} is unknown or already finished")
    await state.store.append_event("run.cancel_requested", entity=str(run_id), payload={"state": outcome})
    return {"id": run_id, "state": outcome}


@post("/apply", opt={"scope": "write"})
async def post_apply(data: ApplyRequest, state: State) -> ApplyResponse:
    compiled: CompiledProject = state.compiled
    env = data.environment or state.environment
    try:
        selected = select_models(data.selectors, compiled) if data.selectors else None
    except SelectionError as exc:
        raise ClientException(detail=exc.message) from exc
    async with state.apply_lock:
        if state.streams:  # an apply must see every event the publish path has accepted
            await flush_streams(state.flush_targets, state.stream_log, state.engine)
        plan = await diff(compiled, env, state.store, select=selected, forward_only=data.forward_only)
        breaking = plan.has_breaking_changes
        if breaking and not data.force:
            names = ", ".join(c.name for c in plan.changes if c.category is ChangeCategory.BREAKING)
            raise ClientException(detail=f"plan has breaking changes ({names}); resubmit with force=true")
        if plan.is_empty:
            return ApplyResponse(environment=env, built=[], promoted=0, breaking=False)
        await state.store.append_event("apply.started", entity=env, payload={"models": plan.promote})
        try:
            result = await apply_plan(
                plan,
                compiled=compiled,
                engines=state.engines,
                state=state.store,
                base_path=state.root,
                parallelism=state.parallelism,
            )
        except CheckError as exc:
            await state.store.append_event("apply.blocked", entity=env, payload={"reason": exc.message})
            raise ClientException(detail=exc.message) from exc
        await state.store.append_event(
            "apply.finished", entity=env, payload={"built": result.built, "promoted": result.promoted}
        )
    return ApplyResponse(
        environment=env,
        built=result.built,
        promoted=result.promoted,
        breaking=breaking,
        reused=result.reused,
        transfers=result.transfers,
        rows={
            name: {"inserted": c.inserted, "updated": c.updated, "deleted": c.deleted}
            for name, c in result.rows.items()
        },
        timings={name: round(seconds, 3) for name, seconds in result.timings.items()},
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
                on_schema_drift=stream.on_schema_drift,
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
    quarantined: list[tuple[object, str]] = []
    try:
        if stream.on_schema_drift == "evolve":
            validate_rows_evolve(stream, rows)  # unknown fields welcome; incompatible types still reject
        elif stream.on_schema_drift == "quarantine":
            rows, quarantined = partition_rows(stream, rows)
        else:
            validate_rows(stream, rows)
    except StreamError as exc:
        raise ClientException(detail=exc.message) from exc

    def _event(row: dict) -> Event:
        key = str(row[stream.idempotency_key]) if stream.idempotency_key and stream.idempotency_key in row else None
        return Event(payload=row, idempotency_key=key)

    result = await state.stream_log.append(name, [_event(row) for row in rows]) if rows else None
    if quarantined:  # failures are durable too: the shadow stream keeps error + raw payload
        shadow = quarantine_stream(stream)
        await state.stream_log.append(
            shadow.name, [Event(payload={"error": error, "payload": json.dumps(row)}) for row, error in quarantined]
        )
    if result or quarantined:  # durable: hand materialization to the flusher micro-batch
        state.flush_wanted.set()
    return PublishResult(
        accepted=result.deduped.count(False) if result else 0,
        deduplicated=result.deduped.count(True) if result else 0,
        last_offset=max(result.offsets) if result and result.offsets else None,
        quarantined=len(quarantined),
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
        result = await run_gc(state.store, engines=state.engines, grace=grace, dry_run=request.dry_run)
    if not request.dry_run:
        await state.store.trim_logs()  # event_log / check_results / terminal queue rows
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
        # Subscribe FIRST so nothing lands between the backlog read and the live
        # tail (the seq guard drops anything the replay already delivered), then
        # replay history from the store and switch to the shared broadcast.
        queue: asyncio.Queue[dict | None] = asyncio.Queue(maxsize=512)
        state.sse_subscribers.add(queue)
        cursor = after
        try:
            while True:
                backlog = await state.store.read_events(cursor)
                if not backlog:
                    break
                for event in backlog:
                    cursor = int(event["seq"])
                    yield ServerSentEventMessage(data=json.dumps(event), event=str(event["type"]), id=str(cursor))
            while True:
                event = await queue.get()
                if event is None:  # poisoned: we fell behind — end the stream, the client replays on reconnect
                    return
                if int(event["seq"]) <= cursor:
                    continue
                cursor = int(event["seq"])
                yield ServerSentEventMessage(data=json.dumps(event), event=str(event["type"]), id=str(cursor))
        finally:
            state.sse_subscribers.discard(queue)

    return ServerSentEvent(tail())


def _broadcast(subscribers: set[asyncio.Queue], event: dict) -> None:
    """Fan one event out to every SSE subscriber. A client that can't keep up is
    poisoned and dropped — its EventSource reconnects with Last-Event-ID and
    replays from the store, so nothing is lost, and one stalled TCP connection
    can't grow a queue forever."""
    for queue in list(subscribers):
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            subscribers.discard(queue)
            with contextlib.suppress(asyncio.QueueEmpty, asyncio.QueueFull):
                queue.get_nowait()  # make room so the poison pill always lands
                queue.put_nowait(None)


def create_app(
    root: Path | str,
    environment: str = "prod",
    quack: str | None = None,
    quack_token: str | None = None,
    scheduler: bool = False,
    scheduler_interval: float = 60.0,
    stream_flush_interval: float = 0.05,
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
        engines = project.open_engines()
        engine = engines.get()  # default warehouse: streams, quack, legacy single-engine paths
        if quack:
            from interlace.engines.quack import QuackAdapter, sql_literal

            if isinstance(engine, QuackAdapter):
                raise ImproperlyConfiguredException(detail="cannot re-serve a quack-connected warehouse")
            token_sql = f", token := {sql_literal(quack_token)}" if quack_token else ""
            await engine.execute_sql(f"CALL quack_serve({sql_literal(quack)}{token_sql})")
        compiled = project.compile()
        stream_log = await project.open_stream_log()
        streams = {stream.name: stream for stream in project.streams}
        shadows = [s for s in streams.values() if s.on_schema_drift == "quarantine"]
        flush_targets = [*streams.values(), *(quarantine_stream(s) for s in shadows)]
        if streams:
            await ensure_stream_tables(flush_targets, engine)
        app.state.compiled = compiled
        app.state.lineage = column_lineage(compiled)  # whole-project qualify: compute once, not per request
        app.state.store = store
        app.state.engine = engine
        app.state.engines = engines
        app.state.environment = environment
        app.state.root = project.root
        app.state.parallelism = project.config.parallelism
        app.state.apply_lock = asyncio.Lock()  # serialise applies against the single warehouse connection
        app.state.streams = streams
        app.state.stream_log = stream_log
        app.state.flush_targets = flush_targets  # streams + their quarantine shadows
        app.state.flush_wanted = asyncio.Event()
        app.state.sse_subscribers = set()

        logger = logging.getLogger("interlace.service")

        # Background loops NEVER die on an exception: one transient warehouse error
        # must not silently stop flushing/scheduling for the rest of the process
        # (publishes would keep acking 200 while nothing materializes). Log + retry.
        async def event_tail() -> None:
            """One store poller feeds every SSE client — N clients, one query.

            The store is polled (not hooked) because other processes — a CLI
            apply against the same project — also append events this daemon
            must surface.
            """
            cursor = await store.latest_event_seq()  # history is each client's replay, not ours
            while True:
                try:
                    if app.state.sse_subscribers:
                        for event in await store.read_events(cursor):
                            cursor = int(event["seq"])  # type: ignore[call-overload]  # rows carry int seq
                            _broadcast(app.state.sse_subscribers, event)
                except Exception:
                    logger.exception("event tail failed; retrying")
                await asyncio.sleep(0.5)

        async def flush_once() -> None:
            """One coalesced flush + the consumer enqueues it earns."""
            async with app.state.apply_lock:
                flushed = await flush_streams(flush_targets, stream_log, engine)
            for stream_name, rows in flushed.items():
                await store.append_event("stream.flushed", entity=stream_name, payload={"rows": rows})
                if stream_name in streams:
                    await _enqueue_stream_consumers(app.state, streams[stream_name])

        async def flusher_loop() -> None:
            """Micro-batch materializer: publishes signal, this coalesces everything
            appended since the last flush into one warehouse write."""
            while True:
                await app.state.flush_wanted.wait()
                await asyncio.sleep(stream_flush_interval)  # let a burst pile up behind one write
                app.state.flush_wanted.clear()
                try:
                    await flush_once()
                except Exception:
                    logger.exception("stream flush failed; will retry")
                    app.state.flush_wanted.set()  # the durable log still holds the events
                    await asyncio.sleep(1.0)  # don't spin on a persistent failure

        if streams:
            app.state.flush_wanted.set()  # catch up anything durable but unflushed at last shutdown

        async def scheduler_loop() -> None:
            trigger_engine = TriggerEngine(build_triggers(compiled), store)
            next_trim = asyncio.get_running_loop().time()  # first tick trims; then every 6h
            while True:
                try:
                    await trigger_engine.tick(datetime.now())
                    if asyncio.get_running_loop().time() >= next_trim:
                        # event_log / check_results / terminal queue rows grow with
                        # every apply and flush; nothing else reclaims them
                        await store.trim_logs()
                        next_trim = asyncio.get_running_loop().time() + 6 * 3600
                    async with app.state.apply_lock:  # one warehouse writer at a time
                        await drain(
                            store,
                            compiled,
                            engines=engines,
                            environment=environment,
                            base_path=project.root,
                            parallelism=project.config.parallelism,
                        )
                    if streams:
                        app.state.flush_wanted.set()  # catch up anything the flusher hasn't seen
                        await sweep_streams(streams.values(), stream_log, engine)  # apply retention
                except Exception:
                    logger.exception("scheduler tick failed; retrying next interval")
                await asyncio.sleep(scheduler_interval)

        tail_task = asyncio.create_task(event_tail())
        flusher_task = asyncio.create_task(flusher_loop()) if streams else None
        loop_task = asyncio.create_task(scheduler_loop()) if scheduler else None
        try:
            yield
        finally:
            for task in (loop_task, flusher_task, tail_task):
                if task is not None:
                    task.cancel()
                    # suppress Exception too: a task that already died must not
                    # abort teardown and leak the store/log/engine handles
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
            if streams:  # clean shutdown leaves nothing durable-but-unflushed behind
                with contextlib.suppress(Exception):
                    await flush_once()  # incl. consumer enqueues, so restarts owe nothing
            await stream_log.close()
            await store.close()
            engines.close()

    ui_router = create_static_files_router(
        path="/ui", directories=[Path(__file__).parent / "ui"], html_mode=True, include_in_schema=False
    )
    return Litestar(
        route_handlers=[
            ui_router,
            ui_redirect,
            health,
            get_models,
            get_model,
            get_plan,
            get_environments,
            drop_environment_endpoint,
            get_runs,
            get_run,
            create_run,
            cancel_run,
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
