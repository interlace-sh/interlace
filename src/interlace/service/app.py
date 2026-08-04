"""The interlace HTTP API (Litestar).

A read + trigger surface over a project: list/inspect models, preview a plan
(with per-change SQL + impacted columns), apply it (build changed snapshots and
promote — guarded against breaking changes unless forced), inspect environments,
list/inspect runs, and enqueue runs onto the durable queue (a running ``interlace
scheduler`` drains them). The project is loaded and compiled once at startup and
held on app state; the warehouse engine and control-plane store are opened for
the app's lifetime. msgspec structs are the wire types (Litestar serialises them
natively). Scoped API-key auth is enforced once a key exists (see auth.py), and
OpenAPI docs render via Scalar at ``/schema/scalar``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import msgspec
import pyarrow as pa
from litestar import Litestar, Request, delete, get, post
from litestar.datastructures import CacheControlHeader, State
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
from interlace.graph.selectors import select_models, wants_state
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
    engine: str = "default"
    language: str = "sql"  # "sql" | "python"


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
    language: str = "sql"  # "sql" | "python"
    source: str | None = None  # dedented function source for Python models


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
    pending: int  # head - watermark: durable events not yet in the warehouse
    on_schema_drift: str = "reject"
    retention: str | None = None  # age after which materialized events are swept


class StreamDetail(msgspec.Struct):
    name: str
    schema: dict[str, str]
    table: str
    head: int
    watermark: int
    pending: int
    idempotency_key: str | None
    recent: list[dict]  # latest payloads, newest last
    on_schema_drift: str = "reject"
    retention: str | None = None


class PublishResult(msgspec.Struct):
    """Ack for a durable append. Materialization is micro-batched: a flusher task
    coalesces publishes into one warehouse write moments later — poll the stream's
    ``watermark`` (GET /streams/{name}) to observe it land."""

    accepted: int
    deduplicated: int
    last_offset: int | None
    quarantined: int = 0  # events diverted to <stream>__quarantine (quarantine mode)


class QueryRequest(msgspec.Struct):
    sql: str
    limit: int = 500  # capped at 10_000; the console is for inspection, not extraction


class QueryResponse(msgspec.Struct):
    columns: list[str]
    types: list[str]
    rows: list[list]  # JSON-safe cells (non-scalar values stringified)
    row_count: int
    truncated: bool
    elapsed_ms: float


class EngineInfo(msgspec.Struct):
    name: str
    type: str
    dialect: str
    database: str  # credentials redacted
    default: bool


class ScheduleInfo(msgspec.Struct):
    model: str
    kind: str  # "cron" | "every"
    expression: str
    next_fire: str | None
    last_fired: str | None


class ImpactColumn(msgspec.Struct):
    model: str
    column: str
    via: str  # the upstream column it was derived from, one hop up


class ImpactResponse(msgspec.Struct):
    source: str  # "model.column"
    impacted: list[ImpactColumn]  # downstream columns transitively derived from it
    opaque_consumers: list[str]  # models reading the source whole (Python / * projections)


class LineageModel(msgspec.Struct):
    name: str
    output: str
    strategy: str
    engine: str
    tags: list[str]
    columns: list[str]  # output columns: warehouse-described, else parsed from the AST
    types: dict[str, str] = msgspec.field(default_factory=dict)  # column -> engine type (when described)
    has_schedule: bool = False
    has_checks: bool = False


class LineageStream(msgspec.Struct):
    name: str  # keyed "streams.<name>" in edges/columns to match SQL table refs
    stream: str  # the bare stream name
    columns: list[str]
    types: dict[str, str] = msgspec.field(default_factory=dict)
    consumers: list[str] = msgspec.field(default_factory=list)  # models reading it directly


class LineageResponse(msgspec.Struct):
    models: list[LineageModel]
    edges: list[list[str]]  # [upstream, downstream]
    # model -> column -> [[upstream_model, upstream_column], ...]
    columns: dict[str, dict[str, list[list[str]]]]
    streams: list[LineageStream] = msgspec.field(default_factory=list)


class RunChecksRequest(msgspec.Struct):
    environment: str | None = None
    selectors: list[str] = msgspec.field(default_factory=list)


class CheckOutcomeInfo(msgspec.Struct):
    model: str
    name: str
    check_type: str
    severity: str
    status: str
    failures: int
    message: str | None = None


class RunChecksResponse(msgspec.Struct):
    environment: str
    outcomes: list[CheckOutcomeInfo]
    skipped: list[str]  # declared but not promoted in this environment
    passed: int
    blocking_failures: int


class ApiKeyInfo(msgspec.Struct):
    name: str
    scopes: list[str]
    created_at: str


class CreateApiKey(msgspec.Struct):
    name: str
    scopes: list[str] = msgspec.field(default_factory=lambda: ["read"])


class RollbackRequest(msgspec.Struct):
    generation: int | None = None  # default: the generation before the latest


class GcRequest(msgspec.Struct):
    grace: str = "7d"  # keep unreferenced snapshots younger than this
    dry_run: bool = False


class GcResponse(msgspec.Struct):
    removed_snapshots: int
    dropped_tables: list[str]
    kept_snapshots: int
    dry_run: bool


def _python_source(model: CompiledModel) -> str | None:
    if model.fn is None:
        return None
    import inspect
    import textwrap

    try:
        return textwrap.dedent(inspect.getsource(model.fn))
    except (OSError, TypeError):  # source unavailable (REPL, C ext): the name still renders
        return None


def _output(model: CompiledModel) -> str:
    return "sink" if model.export is not None else model.materialise


def _info(model: CompiledModel) -> ModelInfo:
    return ModelInfo(
        name=model.name,
        output=_output(model),
        materialise=model.materialise,
        # sinks: the delivery mode is the strategy people reason about (CLI parity)
        strategy=model.export.mode if model.export is not None else model.strategy,
        is_sink=model.export is not None,
        fingerprint=model.fingerprint,
        depends_on=list(model.dependencies),
        tags=list(model.tags),
        owner=model.owner,
        schedule=model.schedule,
        engine=model.engine,
        language="python" if model.ast is None else "sql",
    )


@get("/health")
async def health(state: State) -> dict[str, str]:
    return {"status": "ok", "version": __version__, "environment": state.environment}


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
        language="python" if model.ast is None else "sql",
        source=_python_source(model),
    )


@get("/models/{name:str}/impact")
async def get_model_impact(name: FromPath[str], state: State, column: FromQuery[str]) -> ImpactResponse:
    """Column-level blast radius of ``{name}.{column}``: every downstream column
    transitively derived from it, plus opaque consumers (Python / ``*`` models)."""
    from interlace.graph.column_lineage import column_impact

    compiled: CompiledProject = state.compiled
    if name not in compiled.models:
        raise NotFoundException(detail=f"unknown model: {name}")
    result = column_impact(compiled, name, column)
    return ImpactResponse(
        source=result["source"],
        impacted=[ImpactColumn(**row) for row in result["impacted"]],
        opaque_consumers=result["opaque_consumers"],
    )


@get("/plan")
async def get_plan(
    state: State,
    environment: FromQuery[str | None] = None,
    select: FromQuery[str | None] = None,
    forward_only: FromQuery[bool] = False,
) -> PlanResponse:
    """Preview a plan. ``select`` takes comma-separated selectors (same grammar as
    POST /apply's ``selectors``); ``forward_only`` previews history-inheriting plans
    — so what you preview is exactly what POST /apply will do."""
    env = environment or state.environment
    compiled: CompiledProject = state.compiled
    selectors = [part.strip() for part in select.split(",") if part.strip()] if select else []
    try:
        promoted = await state.store.get_environment(env) if wants_state(selectors) else None
        selected = select_models(selectors, compiled, promoted=promoted) if selectors else None
    except SelectionError as exc:
        raise ClientException(detail=exc.message) from exc
    plan = await diff(compiled, env, state.store, select=selected, forward_only=forward_only)
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


@get("/environments/{name:str}/history")
async def get_environment_history(name: FromPath[str], state: State) -> list[dict]:
    """Promotion generations, newest first — the rollback targets."""
    return [dict(row) for row in await state.store.list_generations(name)]


@post("/environments/{name:str}/rollback", opt={"scope": "admin"}, status_code=200)
async def rollback_environment_endpoint(name: FromPath[str], state: State, data: RollbackRequest | None = None) -> dict:
    """Repoint the environment's views at an earlier promotion generation.
    Nothing rebuilds — the views move."""
    from interlace.exceptions import PlanError
    from interlace.state.janitor import rollback_environment

    request = data or RollbackRequest()
    try:
        async with state.apply_lock:
            result = await rollback_environment(
                state.store,
                engines=state.engines,
                environment=name,
                to_generation=request.generation,
            )
    except PlanError as exc:
        raise ClientException(detail=exc.message) from exc
    await state.store.append_event("environment.rolled_back", entity=name, payload=result)
    return result


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
        promoted = await state.store.get_environment(env) if wants_state(data.selectors) else None
        selected = (
            select_models(data.selectors, compiled, promoted=promoted) if data.selectors else set(compiled.models)
        )
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
        promoted = await state.store.get_environment(env) if wants_state(data.selectors) else None
        selected = select_models(data.selectors, compiled, promoted=promoted) if data.selectors else None
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
        loop = asyncio.get_running_loop()
        progress_tasks: set[asyncio.Task] = set()

        def on_progress(model: str, event: str) -> None:
            # fire-and-forget telemetry, but hold a strong ref: an unreferenced task
            # can be GC'd mid-write and its exception silently vanishes
            task = loop.create_task(
                state.store.append_event(f"model.{event}", entity=model, payload={"environment": env})
            )
            progress_tasks.add(task)
            task.add_done_callback(progress_tasks.discard)

        try:
            result = await apply_plan(
                plan,
                compiled=compiled,
                engines=state.engines,
                state=state.store,
                base_path=state.root,
                parallelism=state.parallelism,
                on_progress=on_progress,
            )
        except CheckError as exc:
            await state.store.append_event("apply.blocked", entity=env, payload={"reason": exc.message})
            raise ClientException(detail=exc.message) from exc
        finally:
            if progress_tasks:  # let the per-model events land before we return / raise
                await asyncio.gather(*progress_tasks, return_exceptions=True)
        await state.store.append_event(
            "apply.finished", entity=env, payload={"built": result.built, "promoted": result.promoted}
        )
        state.describe_cache.clear()  # new fingerprints, new tables: /lineage re-describes
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
    consumers = state.stream_consumer_map.get(stream.name, [])
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
        head = await state.stream_log.head(stream.name)
        watermark = await stream_watermark(stream, state.engine)
        out.append(
            StreamInfo(
                name=stream.name,
                schema=stream.schema,
                table=f"streams.{stream.name}",
                head=head,
                watermark=watermark,
                pending=max(0, head - watermark),
                on_schema_drift=stream.on_schema_drift,
                retention=stream.retention,
            )
        )
    return out


@get("/streams/{name:str}")
async def get_stream(name: FromPath[str], state: State) -> StreamDetail:
    stream = _stream_or_404(state, name)
    head = await state.stream_log.head(name)
    watermark = await stream_watermark(stream, state.engine)
    events = await state.stream_log.read(name, max(0, head - 20), 20)
    return StreamDetail(
        name=stream.name,
        schema=stream.schema,
        table=f"streams.{stream.name}",
        head=head,
        watermark=watermark,
        pending=max(0, head - watermark),
        idempotency_key=stream.idempotency_key,
        recent=[dict(event.payload, _offset=event.offset) for event in events],
        on_schema_drift=stream.on_schema_drift,
        retention=stream.retention,
    )


@post("/streams/{name:str}", opt={"scope": "write"})
async def publish(name: FromPath[str], data: dict | list, state: State) -> PublishResult:
    """Publish one event (object) or a batch (array). Durable before this returns.

    Backpressure: when the warehouse can't keep up, the durable-but-unmaterialized
    backlog grows without bound — past ``stream_max_pending`` this returns 429 so
    producers slow down instead of the daemon eating memory and disk. The counters
    are maintained by the publish/flush paths themselves; nothing here queries the
    warehouse.
    """
    stream = _stream_or_404(state, name)
    shadow_name = f"{name}__quarantine"
    pending = max(
        state.log_heads.get(name, 0) - state.flushed_heads.get(name, 0),
        state.log_heads.get(shadow_name, 0) - state.flushed_heads.get(shadow_name, 0),
    )
    if pending > state.stream_max_pending:
        from litestar.exceptions import HTTPException

        raise HTTPException(
            status_code=429,
            detail=f"stream {name!r} has {pending} unmaterialized events (limit {state.stream_max_pending}) — "
            f"the warehouse is behind; retry with backoff",
        )
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
        shadow_result = await state.stream_log.append(
            shadow.name, [Event(payload={"error": error, "payload": json.dumps(row)}) for row, error in quarantined]
        )
        if shadow_result.offsets:  # the shadow log is backpressured like any other
            state.log_heads[shadow.name] = max(state.log_heads.get(shadow.name, 0), max(shadow_result.offsets))
    if result and result.offsets:
        state.log_heads[name] = max(state.log_heads.get(name, 0), max(result.offsets))
    if result or quarantined:  # durable: hand materialization to the flusher micro-batch
        state.flush_dirty.add(name)
        if quarantined:
            state.flush_dirty.add(quarantine_stream(stream).name)
        state.flush_wanted.set()
    return PublishResult(
        accepted=result.deduped.count(False) if result else 0,
        deduplicated=result.deduped.count(True) if result else 0,
        last_offset=max(result.offsets) if result and result.offsets else None,
        quarantined=len(quarantined),
    )


def _jsonable(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)  # timestamps, decimals, structs — stringified for the wire


# DuckDB table functions and scalar functions that read OUTSIDE the warehouse —
# the console must never become a local-file reader or an HTTP client. Deny by
# name pattern; the warehouse's own tables/views never match these shapes.
_EXTERNAL_FN = re.compile(
    r"^(read_|scan_|sniff_|glob$|getenv$|load_|install$|parquet_|iceberg_|delta_|st_read|query$|query_table$)"
    r"|(_scan|_query)$",
    re.IGNORECASE,
)
_QUERY_MAX_CELL_BYTES = 8_000_000  # ~8 MB of rendered cells; the console inspects, never extracts


def _guard_console_query(parsed: object) -> None:
    from sqlglot import exp as _exp

    for node in parsed.walk():  # type: ignore[attr-defined]
        name = None
        if isinstance(node, _exp.Anonymous):
            name = str(node.this)
        elif isinstance(node, _exp.Func):
            name = node.sql_name()
        if name and _EXTERNAL_FN.search(name):
            raise ClientException(detail=f"function {name!r} reads outside the warehouse — not in the console")
        if isinstance(node, _exp.Table):
            raw = node.name or ""
            if "/" in raw or "\\" in raw or "://" in raw:
                raise ClientException(detail="file paths are not queryable from the console")


@post("/query", opt={"scope": "read"})
async def post_query(data: QueryRequest, state: State) -> QueryResponse:
    """Run a read-only query on the default engine. SELECT only, warehouse only —
    DDL/DML and external readers (files, HTTP) are rejected at parse; a row cap
    and a byte cap are always applied."""
    import time as _time

    import sqlglot
    from sqlglot import exp as _exp

    try:
        statements = sqlglot.parse(data.sql, read=state.engine.dialect)
    except Exception as exc:
        raise ClientException(detail=f"could not parse query: {exc}") from exc
    if len(statements) != 1 or statements[0] is None:
        raise ClientException(detail="exactly one statement, please")
    parsed = statements[0]
    if not isinstance(parsed, (_exp.Select, _exp.Union)):
        raise ClientException(detail="SELECT only — the console never writes")
    _guard_console_query(parsed)  # defense in depth; the sandboxed cursor is the real fence
    limit = max(1, min(data.limit, 10_000))
    bounded = _exp.select("*").from_(parsed.subquery("q")).limit(limit + 1)  # +1: exact truncation
    started = _time.perf_counter()

    async def _run() -> pa.Table:
        # fetch_sandboxed runs with external access disabled: query()/read_csv()/
        # httpfs cannot reach host files or the network however they are spelled
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


@get("/schedules")
async def get_schedules(state: State) -> list[ScheduleInfo]:
    from datetime import datetime

    from cronsim import CronSim

    compiled: CompiledProject = state.compiled
    infos: list[ScheduleInfo] = []
    for name in sorted(compiled.models):
        schedule = compiled.models[name].schedule
        if not schedule:
            continue
        kind = "cron" if "cron" in schedule else "every"
        expression = str(schedule.get("cron") or schedule.get("every"))
        last = await state.store.get_trigger_last_fired(f"{'cron' if kind == 'cron' else 'interval'}:{name}")

        def _wire(moment: datetime | None) -> str | None:
            # trigger timestamps are naive LOCAL (the scheduler ticks datetime.now());
            # attach the local offset or every consumer reads them as UTC
            return moment.astimezone().isoformat() if moment else None

        next_fire: datetime | None = None
        if kind == "cron":
            with contextlib.suppress(Exception):
                next_fire = next(CronSim(expression, last or datetime.now()))
        elif last is not None:
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


def _ast_projection_names(model: CompiledModel) -> list[str]:
    """Output column names read straight off a simple SELECT (no star), else []."""
    from sqlglot import exp as _exp

    if not isinstance(model.ast, _exp.Select):
        return []
    names: list[str] = []
    for projection in model.ast.selects:
        if projection.find(_exp.Star, _exp.Columns) is not None:
            return []
        names.append(projection.alias_or_name)
    return names


async def _described_columns(state: State, name: str, promoted: dict[str, str], snapshots: dict) -> dict[str, str]:
    """Column -> type from the promoted physical table, cached per fingerprint.
    Empty when the model isn't promoted / built yet (compile-time names still show)."""
    fingerprint = promoted.get(name)
    snapshot = snapshots.get((name, fingerprint or ""))
    if snapshot is None:
        return {}
    cache: dict[tuple[str, str | None], dict[str, str]] = state.describe_cache
    key = (name, fingerprint)
    if key not in cache:
        described: dict[str, str] = {}
        with contextlib.suppress(Exception):  # missing table / engine away: names-only is fine
            engine = state.engines.require(snapshot.engine, model=name)
            described = await engine.describe(snapshot.physical_table)
        if not described:
            return {}  # transient (outage, not built yet): never cache a negative
        cache[key] = described
    return cache[key]


@get("/lineage")
async def get_lineage(state: State) -> LineageResponse:
    """The whole graph in one payload: nodes (with warehouse-described column
    types), table edges, column lineage, and stream sources — the UI renders
    and traces without a request per node."""
    compiled: CompiledProject = state.compiled
    promoted = await state.store.get_environment(state.environment)
    snapshots = await state.store.get_snapshots(promoted.items())

    models: list[LineageModel] = []
    edges: list[list[str]] = []
    known = set(compiled.models)
    for name in compiled.graph.topological_sort():
        model = compiled.models[name]
        types = await _described_columns(state, name, promoted, snapshots)
        columns = list(types) or list(state.lineage.get(name, {})) or _ast_projection_names(model)
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
    # (a VALUES alias like `t` is a phantom, not a navigable source)
    column_sources = {
        name: {col: [[t, c] for t, c in refs if t in known or t in stream_keys] for col, refs in sources.items()}
        for name, sources in state.lineage.items()
        if sources
    }
    # models without parsed lineage (Python models, sinks over them) would break
    # every trace passing through: fall back to NAME-MATCHING against direct
    # upstreams — a heuristic, but the honest alternative is a dead end
    columns_of = {info.name: set(info.columns) for info in models}
    for info in models:
        if column_sources.get(info.name):
            continue
        model = compiled.models[info.name]
        matched = {
            col: [[dep, col] for dep in model.dependencies if col in columns_of.get(dep, set())] for col in info.columns
        }
        matched = {col: refs for col, refs in matched.items() if refs}
        if matched:
            column_sources[info.name] = matched
    return LineageResponse(models=models, edges=edges, columns=column_sources, streams=streams)


@post("/checks/run", opt={"scope": "write"})
async def post_checks_run(state: State, data: RunChecksRequest | None = None) -> RunChecksResponse:
    """Run checks ad hoc against an environment's promoted tables (dbt-test style),
    recording the results."""
    from interlace.checks.runner import run_checks

    request = data or RunChecksRequest()
    compiled: CompiledProject = state.compiled
    env = request.environment or state.environment
    promoted = await state.store.get_environment(env)
    try:
        chosen = select_models(request.selectors, compiled, promoted=promoted) if request.selectors else None
    except SelectionError as exc:
        raise ClientException(detail=exc.message) from exc
    if not promoted:
        raise NotFoundException(detail=f"no environment {env!r} — apply first")
    snapshots = await state.store.get_snapshots(promoted.items())
    physical = {name: snapshot.physical_table for (name, _), snapshot in snapshots.items()}
    outcomes: list[CheckOutcomeInfo] = []
    blocking = 0
    skipped: list[str] = []
    for name, model in compiled.models.items():
        if chosen is not None and name not in chosen:
            continue
        if (not model.checks and not compiled.python_checks.get(name)) or model.export is not None:
            continue
        snapshot = snapshots.get((name, promoted.get(name, "")))
        if snapshot is None:
            skipped.append(name)
            continue
        # the SNAPSHOT's engine, not the compiled model's: an engine re-pin since
        # the last promote means the promoted table still lives on the old engine
        engine = state.engines.require(snapshot.engine, model=name)
        results = await run_checks(
            model, compiled, engine, snapshot.physical_table, compiled.python_checks.get(name, ()), physical
        )
        if results:
            await state.store.record_check_results(env, snapshot.fingerprint, results)
        for outcome in results:
            blocking += 1 if outcome.blocking else 0
            outcomes.append(
                CheckOutcomeInfo(
                    model=outcome.model,
                    name=outcome.name,
                    check_type=outcome.type,
                    severity=outcome.severity,
                    status=outcome.status,
                    failures=outcome.failures,
                    message=outcome.message,
                )
            )
    return RunChecksResponse(
        environment=env,
        outcomes=outcomes,
        skipped=sorted(skipped),
        passed=sum(1 for o in outcomes if o.status == "passed"),
        blocking_failures=blocking,
    )


@get("/apikeys", opt={"scope": "admin"})
async def get_apikeys(state: State) -> list[ApiKeyInfo]:
    return [
        ApiKeyInfo(name=str(k["name"]), scopes=[str(s) for s in k["scopes"]], created_at=str(k["created_at"]))
        for k in await state.store.list_api_keys()
    ]


@post("/apikeys", opt={"scope": "admin"})
async def post_apikey(data: CreateApiKey, state: State) -> dict:
    name = data.name.strip()
    if not name:
        raise ClientException(detail="name the key")
    invalid = set(data.scopes) - {"read", "write", "admin"}
    if invalid or not data.scopes:
        raise ClientException(detail=f"scopes must be a non-empty subset of read/write/admin (got {data.scopes})")
    if any(key["name"] == name for key in await state.store.list_api_keys()):
        raise ClientException(detail=f"a key named {name!r} already exists — revoke it first or pick another name")
    token = await state.store.create_api_key(name, data.scopes)
    return {"name": name, "scopes": data.scopes, "token": token}  # token shown once


@delete("/apikeys/{name:str}", opt={"scope": "admin"}, status_code=200)
async def delete_apikey(name: FromPath[str], state: State) -> dict:
    keys = await state.store.list_api_keys()
    matching = sum(1 for key in keys if key["name"] == name)
    if not matching:
        raise NotFoundException(detail=f"no key named {name!r}")
    if matching == len(keys):
        # zero keys = keyless mode = every request is admin. Revoking the last
        # key must never silently disable authentication.
        raise ClientException(
            detail="refusing to revoke the last key(s) — that would disable authentication; "
            "create a replacement first"
        )
    removed = await state.store.revoke_api_key(name)
    return {"name": name, "removed": removed}


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
async def stream_events(state: State, request: Request, after: FromQuery[int] = 0) -> ServerSentEvent:
    # reconnects carry Last-Event-ID (set from the id: field); fresh connections
    # pass ?after= so a page load doesn't replay the whole event log
    after = int(request.headers.get("Last-Event-ID") or after)

    async def tail() -> AsyncIterator[ServerSentEventMessage]:
        # Subscribe FIRST so nothing lands between the backlog read and the live
        # tail (the seq guard drops anything the replay already delivered), then
        # replay history from the store and switch to the shared broadcast.
        queue: asyncio.Queue[tuple[int, str] | None] = asyncio.Queue(maxsize=512)
        state.sse_subscribers.add(queue)
        cursor = after
        try:
            while True:
                backlog = await state.store.read_events(cursor)
                if not backlog:
                    break
                for event in backlog:
                    cursor = int(event["seq"])
                    yield ServerSentEventMessage(data=json.dumps(event), id=str(cursor))
            while True:
                item = await queue.get()
                if item is None:  # poisoned: we fell behind — end the stream, the client replays on reconnect
                    return
                seq, payload = item
                if seq <= cursor:
                    continue
                cursor = seq
                yield ServerSentEventMessage(data=payload, id=str(cursor))
        finally:
            state.sse_subscribers.discard(queue)

    return ServerSentEvent(tail())


def _broadcast(subscribers: set[asyncio.Queue], event: dict) -> None:
    """Fan one event out to every SSE subscriber. A client that can't keep up is
    poisoned and dropped — its EventSource reconnects with Last-Event-ID and
    replays from the store, so nothing is lost, and one stalled TCP connection
    can't grow a queue forever. The wire form is serialised ONCE here, not once
    per client."""
    item = (int(event["seq"]), json.dumps(event))
    for queue in list(subscribers):
        try:
            queue.put_nowait(item)
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
        app.state.engine_configs = project.config.engine_configs()
        app.state.default_engine = project.config.default_engine
        app.state.describe_cache = {}  # (model, fingerprint) -> {column: type}, filled by /lineage
        app.state.apply_lock = asyncio.Lock()  # serialise applies against the single warehouse connection
        app.state.streams = streams
        app.state.stream_log = stream_log
        app.state.flush_targets = flush_targets  # streams + their quarantine shadows
        app.state.flush_wanted = asyncio.Event()
        # dirty set: the flusher only touches streams that actually received a
        # publish since the last flush (idle streams cost zero warehouse queries).
        # Seeded full so the startup catch-up covers anything unflushed at shutdown.
        app.state.flush_dirty = {target.name for target in flush_targets}
        # stream -> consuming models, computed once: stream_consumers walks every
        # model AST, and the answer is immutable for a compiled project
        app.state.stream_consumer_map = {
            stream_name: sorted(stream_consumers(compiled, stream_name)) for stream_name in streams
        }
        # Backpressure gauge: pending = log_heads - flushed_heads per stream. Seeded
        # equal (assume caught up — the startup catch-up flush runs immediately and
        # makes it true); maintained by publish/flush, never by warehouse queries.
        app.state.log_heads = await stream_log.heads() if streams else {}
        app.state.flushed_heads = dict(app.state.log_heads)
        app.state.stream_max_pending = 100_000  # per stream; config knob when someone needs one
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
            """One coalesced flush of the DIRTY streams + the consumer enqueues it earns."""
            dirty = set(app.state.flush_dirty)
            app.state.flush_dirty.clear()  # publishes during the flush re-dirty for the next cycle
            targets = [target for target in flush_targets if target.name in dirty]
            if not targets:
                return
            # snapshot the heads BEFORE the flush: flush_stream drains everything
            # appended up to now, but a publish landing mid-flush advances log_heads
            # without being flushed — crediting that later would undercount pending
            # and defeat the 429 backpressure gate.
            pre_flush = {target.name: app.state.log_heads.get(target.name, 0) for target in targets}
            try:
                async with app.state.apply_lock:
                    flushed = await flush_streams(targets, stream_log, engine)
            except BaseException:
                app.state.flush_dirty |= dirty  # nothing confirmed: keep them dirty
                raise
            for target in targets:
                app.state.flushed_heads[target.name] = pre_flush[target.name]
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
            # Teardown must survive being CANCELLED (a second Ctrl+C, or uvicorn's
            # graceful-shutdown timeout, lands mid-teardown): anyio cancel scopes are
            # level-triggered, so every unshielded await below would raise instantly
            # and the store/log/engine handles would leak — shield the whole thing.
            import anyio

            for task in (loop_task, flusher_task, tail_task):
                if task is not None:
                    task.cancel()
                    # suppress Exception too: a task that already died must not
                    # abort teardown and leak the store/log/engine handles
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
            if streams:  # clean shutdown leaves nothing durable-but-unflushed behind
                with anyio.move_on_after(8, shield=True):  # best-effort, bounded: force-quit must still quit
                    with contextlib.suppress(Exception):
                        await flush_once()  # incl. consumer enqueues, so restarts owe nothing
            with anyio.CancelScope(shield=True):  # closes are fast and MUST run
                with contextlib.suppress(Exception):
                    await stream_log.close()
                with contextlib.suppress(Exception):
                    await store.close()
            engines.close()

    # no-cache (not no-store): the browser may keep copies but must revalidate,
    # so upgrading the daemon can never serve a stale shell against new modules
    ui_router = create_static_files_router(
        path="/ui",
        directories=[Path(__file__).parent / "ui"],
        html_mode=True,
        include_in_schema=False,
        cache_control=CacheControlHeader(no_cache=True),
    )
    from litestar import Response

    from interlace.exceptions import InterlaceError as _InterlaceError

    def _domain_error(request: Request, exc: Exception) -> Response:
        # user-caused errors (bad selector, unknown engine, contract violation)
        # are 4xx with their message — never anonymous 500s
        message = getattr(exc, "message", str(exc))
        status = 404 if "unknown" in message[:40].lower() else 400
        return Response(content={"detail": message}, status_code=status)

    return Litestar(
        exception_handlers={_InterlaceError: _domain_error},
        route_handlers=[
            ui_router,
            ui_redirect,
            health,
            get_models,
            get_model,
            get_model_impact,
            get_plan,
            get_environments,
            drop_environment_endpoint,
            get_environment_history,
            rollback_environment_endpoint,
            get_runs,
            get_run,
            create_run,
            cancel_run,
            post_apply,
            get_checks,
            post_checks_run,
            get_streams,
            get_stream,
            publish,
            post_gc,
            post_query,
            get_engines,
            get_schedules,
            get_lineage,
            get_apikeys,
            post_apikey,
            delete_apikey,
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
