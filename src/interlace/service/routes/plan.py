"""Plan, apply, and run."""

from __future__ import annotations

from litestar import get, post
from litestar.datastructures import State
from litestar.exceptions import ClientException
from litestar.params import FromQuery

from interlace.exceptions import CheckError, SelectionError
from interlace.graph.project import CompiledProject
from interlace.plan.apply import ApplyResult
from interlace.plan.orchestrate import compute_plan, plan_and_apply, resolve_selection, run_and_apply
from interlace.plan.plan import Plan
from interlace.scheduler.daemon import (
    publish_compiled,
    reload_if_stale,
)
from interlace.service.present import (
    _apply_response,
    _event_progress,
    _flush_if_streams,
)
from interlace.service.types import (
    ApplyRequest,
    ApplyResponse,
    Change,
    CreateRun,
    PlanResponse,
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
    await reload_if_stale(state)
    env = environment or state.environment
    compiled: CompiledProject = state.compiled
    selectors = [part.strip() for part in select.split(",") if part.strip()] if select else []
    try:
        selected = await resolve_selection(compiled, state.store, env, selectors)
        plan = await compute_plan(compiled, env, state.store, state.engines, select=selected, forward_only=forward_only)
    except SelectionError as exc:
        raise ClientException(detail=exc.message) from exc
    from interlace.plan.payload import plan_document

    previous_snapshots = await state.store.get_snapshots(
        (c.name, c.previous_fingerprint) for c in plan.changes if c.previous_fingerprint is not None
    )
    document = plan_document(plan, compiled, previous_snapshots, env)
    return PlanResponse(
        environment=document.environment,
        changes=[
            Change(
                name=change.name,
                change_type=change.change_type,
                category=change.category,
                previous_fingerprint=change.previous_fingerprint,
                new_fingerprint=change.new_fingerprint,
                impacted_columns=list(change.impacted_columns),
                new_sql=change.new_sql,
                previous_sql=change.previous_sql,
                reused=change.reused,
            )
            for change in document.changes
        ],
        transfers=list(document.transfers),
        physical=list(document.physical),
        drift=list(document.drift),
    )


@post("/apply", opt={"scope": "write"})
async def post_apply(data: ApplyRequest, state: State) -> ApplyResponse:
    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    env = data.environment or state.environment
    on_progress, drain_progress = _event_progress(state, {"environment": env})

    async def on_start(plan: Plan) -> None:
        await state.store.append_event("apply.started", entity=env, payload={"models": plan.promote})

    async def on_finish(result: ApplyResult) -> None:
        await state.store.append_event(
            "apply.finished",
            entity=env,
            payload={"built": result.built, "promoted": result.promoted},
        )
        state.describe_cache.clear()

    try:
        plan, result = await plan_and_apply(
            compiled,
            environment=env,
            project=state.project,
            engines=state.engines,
            state=state.store,
            lock_owner=state.lock_owner,
            selectors=data.selectors,
            forward_only=data.forward_only,
            force=data.force,
            parallelism=state.parallelism,
            connections=state.connections,
            on_progress=on_progress,
            on_compiled=lambda fresh: publish_compiled(state, fresh),
            on_start=on_start,
            on_finish=on_finish,
            prepare=lambda: _flush_if_streams(state),
        )
    except CheckError as exc:
        await state.store.append_event("apply.blocked", entity=env, payload={"reason": exc.message})
        raise ClientException(detail=exc.message) from exc
    finally:
        await drain_progress()
    return _apply_response(env, result, breaking=plan.has_breaking_changes)


@post("/run", opt={"scope": "write"})
async def post_run(data: CreateRun, state: State) -> ApplyResponse:
    """Force-build models and promote immediately — CLI ``interlace run`` / ``restate``
    parity. ``POST /runs`` remains the fire-and-forget enqueue path for the scheduler."""
    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    env = data.environment or state.environment
    from datetime import datetime

    from interlace.state.interval import naive_local

    def _bound(value: str | None) -> datetime | None:
        if not value:
            return None
        return naive_local(value)

    try:
        window_start, window_end = _bound(data.start), _bound(data.end)
    except ValueError as exc:
        raise ClientException(detail=f"start/end must be ISO timestamps: {exc}") from exc

    event = "restate.started" if data.restate else "run.started"
    on_progress, drain_progress = _event_progress(state, {"environment": env})

    async def on_start(plan: Plan) -> None:
        await state.store.append_event(event, entity=env, payload={"models": plan.promote, "restate": data.restate})

    async def on_finish(result: ApplyResult) -> None:
        await state.store.append_event(
            "run.finished",
            entity=env,
            payload={"built": result.built, "promoted": result.promoted},
        )
        state.describe_cache.clear()

    try:
        _plan, result = await run_and_apply(
            compiled,
            environment=env,
            project=state.project,
            engines=state.engines,
            state=state.store,
            lock_owner=state.lock_owner,
            selectors=data.selectors,
            start=window_start,
            end=window_end,
            restate=data.restate,
            parallelism=state.parallelism,
            connections=state.connections,
            on_progress=on_progress,
            on_compiled=lambda fresh: publish_compiled(state, fresh),
            on_start=on_start,
            on_finish=on_finish,
            prepare=lambda: _flush_if_streams(state),
        )
    except CheckError as exc:
        await state.store.append_event("run.blocked", entity=env, payload={"reason": exc.message})
        raise ClientException(detail=exc.message) from exc
    except SelectionError as exc:
        raise ClientException(detail=exc.message) from exc
    finally:
        await drain_progress()
    return _apply_response(env, result, breaking=False)
