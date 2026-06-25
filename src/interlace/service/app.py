"""The interlace HTTP API (Litestar).

A read + trigger surface over a project: list/inspect models, preview a plan,
list runs, and enqueue runs onto the durable queue (a running ``interlace
scheduler`` drains them). The project is loaded and compiled once at startup and
held on app state; the warehouse engine and control-plane store are opened for
the app's lifetime. msgspec structs are the wire types (Litestar serializes them
natively). Scoped API-key auth is enforced once a key exists (see auth.py), and
OpenAPI docs render via Scalar at ``/schema/scalar``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import msgspec
from litestar import Litestar, Request, get, post
from litestar.datastructures import State
from litestar.exceptions import ClientException, NotFoundException
from litestar.openapi import OpenAPIConfig
from litestar.openapi.plugins import ScalarRenderPlugin
from litestar.params import FromPath, FromQuery
from litestar.response import ServerSentEvent, ServerSentEventMessage

from interlace import __version__
from interlace.exceptions import SelectionError
from interlace.graph.column_lineage import column_lineage
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.graph.selectors import select_models
from interlace.plan.differ import diff
from interlace.project import Project
from interlace.service.auth import auth_guard


class ModelInfo(msgspec.Struct):
    name: str
    output: str
    strategy: str
    depends_on: list[str]


class ModelDetail(msgspec.Struct):
    name: str
    output: str
    strategy: str
    depends_on: list[str]
    upstream: list[str]
    downstream: list[str]
    columns: dict[str, list[str]]


class Change(msgspec.Struct):
    name: str
    change_type: str
    category: str | None


class PlanResponse(msgspec.Struct):
    environment: str
    changes: list[Change]


class RunInfo(msgspec.Struct):
    id: int
    flow_selector: list[str]
    state: str
    attempts: int
    error: str | None


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


def _output(model: CompiledModel) -> str:
    return "sink" if model.export is not None else model.materialise


def _info(model: CompiledModel) -> ModelInfo:
    return ModelInfo(model.name, _output(model), model.strategy, list(model.dependencies))


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
        strategy=model.strategy,
        depends_on=list(model.dependencies),
        upstream=sorted(compiled.graph.ancestors(name)),
        downstream=sorted(compiled.graph.descendants(name)),
        columns={col: [f"{t}.{c}" for t, c in refs] for col, refs in cols.items()},
    )


@get("/plan")
async def get_plan(state: State, environment: FromQuery[str | None] = None) -> PlanResponse:
    env = environment or state.environment
    plan = await diff(state.compiled, env, state.store)
    return PlanResponse(
        environment=env,
        changes=[Change(c.name, c.change_type.value, c.category.value if c.category else None) for c in plan.changes],
    )


@get("/runs")
async def get_runs(state: State) -> list[RunInfo]:
    return [RunInfo(**run) for run in await state.store.list_runs()]


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


def create_app(root: Path | str, environment: str = "dev") -> Litestar:
    """Build the Litestar app for the project at ``root``."""

    @asynccontextmanager
    async def lifespan(app: Litestar) -> AsyncIterator[None]:
        project = Project.load(root)
        store = await project.open_state()
        engine = project.open_engine()
        app.state.compiled = project.compile()
        app.state.store = store
        app.state.engine = engine
        app.state.environment = environment
        try:
            yield
        finally:
            await store.close()
            engine.close()

    return Litestar(
        route_handlers=[health, get_models, get_model, get_plan, get_runs, create_run, get_events, stream_events],
        lifespan=[lifespan],
        guards=[auth_guard],
        openapi_config=OpenAPIConfig(
            title="interlace",
            version=__version__,
            description="Python/SQL-first data platform: transformation, orchestration, and streaming.",
            render_plugins=[ScalarRenderPlugin()],
        ),
    )
