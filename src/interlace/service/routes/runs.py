"""Run queue: list, detail, enqueue, hook, and cancel."""

from __future__ import annotations

from uuid import uuid4

from litestar import Request, get, post
from litestar.datastructures import State
from litestar.exceptions import ClientException, NotFoundException
from litestar.params import FromPath, FromQuery

from interlace.exceptions import SelectionError
from interlace.graph.project import CompiledProject
from interlace.plan.orchestrate import resolve_selection
from interlace.scheduler.daemon import (
    reload_if_stale,
)
from interlace.service.types import (
    CreateRun,
    CreateRunResult,
    EventInfo,
    HookResult,
    RunDetail,
    RunInfo,
)


@get("/runs")
async def get_runs(state: State, limit: FromQuery[int | None] = None) -> list[RunInfo]:
    runs: list[RunInfo] = []
    for run in await state.store.list_runs():
        partition = [str(run["partition_start"]), str(run["partition_end"])] if run["partition_start"] else None
        started, finished = run.get("started_at"), run.get("finished_at")
        duration = None
        if started and finished:  # ledger timestamps are naive ISO; the span is what we want
            from datetime import datetime

            duration = (datetime.fromisoformat(finished) - datetime.fromisoformat(started)).total_seconds()
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
                environment=run.get("environment"),
                duration=duration,
            )
        )
    return runs[:limit] if limit else runs  # list_runs is newest-first


@get("/runs/{run_id:int}")
async def get_run(run_id: FromPath[int], state: State) -> RunDetail:
    run = await state.store.get_run(run_id)
    if run is None:
        raise NotFoundException(detail=f"unknown run: {run_id}")
    # lifecycle events are keyed by run id (worker) and idempotency key (enqueue); the
    # per-model events are keyed by payload.run (their entity is the model name)
    events = await state.store.events_for_entity(str(run_id)) + await state.store.events_for_run(run_id)
    if run["idempotency_key"]:
        events += await state.store.events_for_entity(run["idempotency_key"])
    events = sorted({event["seq"]: event for event in events}.values(), key=lambda event: event["seq"])
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
    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    env = data.environment or state.environment
    try:
        selected = await resolve_selection(compiled, state.store, env, data.selectors, default_all=True)
    except SelectionError as exc:
        raise ClientException(detail=exc.message) from exc
    models = sorted(selected or ())
    partition = None
    if data.start or data.end:
        from interlace.state.interval import naive_local

        def naive(value: str) -> str:
            return naive_local(value).isoformat()

        try:
            bounds = tuple(naive(v) if v else "" for v in (data.start, data.end))
        except ValueError as exc:
            raise ClientException(detail=f"start/end must be ISO timestamps: {exc}") from exc
        partition = (bounds[0] or None, bounds[1] or None)
    key = f"api:{env}:{uuid4().hex}"
    enqueued = await state.store.enqueue_run(key, models, partition, 0, restate=data.restate)
    if enqueued:
        await state.store.append_event("run.enqueued", entity=key, payload={"models": models})
        state.drain_wanted.set()  # wake the scheduler now, don't wait out the tick interval
    return CreateRunResult(enqueued=1 if enqueued else 0, models=models)


@post("/hooks/{name:str}", opt={"scope": "write"}, status_code=201)
async def post_hook(name: FromPath[str], state: State, request: Request) -> HookResult:
    """Enqueue the model that declares ``schedule: {webhook: name}``.

    ``Idempotency-Key`` dedupes a retried delivery. Without it, every POST is a new run.
    """
    from interlace.exceptions import DefinitionError
    from interlace.scheduler.engine import scheduled_closure, webhook_targets

    await reload_if_stale(state)
    try:
        targets = webhook_targets(state.compiled)
    except DefinitionError as exc:
        raise ClientException(detail=exc.message) from exc
    model = targets.get(name)
    if model is None:
        raise NotFoundException(detail=f"unknown webhook: {name}")
    supplied = request.headers.get("Idempotency-Key", "").strip()
    key = supplied or f"webhook:{name}:{uuid4().hex}"
    models = scheduled_closure(state.compiled, [model])
    enqueued = await state.store.enqueue_run(key, models, None)
    if enqueued:
        await state.store.append_event("run.enqueued", entity=key, payload={"models": models, "webhook": name})
        state.drain_wanted.set()
    return HookResult(model=model, idempotency_key=key, enqueued=enqueued)


@post("/runs/{run_id:int}/cancel", opt={"scope": "write"}, status_code=200)
async def cancel_run(run_id: FromPath[int], state: State) -> dict:
    """Cancel a run: queued cancels immediately; running cancels cooperatively
    at the worker's next heartbeat."""
    outcome = await state.store.request_cancel(run_id)
    if outcome is None:
        raise NotFoundException(detail=f"run {run_id} is unknown or already finished")
    await state.store.append_event("run.cancel_requested", entity=str(run_id), payload={"state": outcome})
    return {"id": run_id, "state": outcome}
