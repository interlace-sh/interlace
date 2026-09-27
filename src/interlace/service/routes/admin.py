"""Checks, API keys, garbage collection, reset, and the event tail."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path

from litestar import Request, delete, get, post
from litestar.datastructures import State
from litestar.exceptions import ClientException, NotFoundException
from litestar.params import FromPath, FromQuery
from litestar.response import ServerSentEvent, ServerSentEventMessage

from interlace.exceptions import SelectionError
from interlace.graph.project import CompiledProject
from interlace.graph.selectors import select_models
from interlace.scheduler.daemon import (
    reload_if_stale,
)
from interlace.service.types import (
    ApiKeyInfo,
    CheckOutcomeInfo,
    CheckResultInfo,
    CreateApiKey,
    EventInfo,
    FixtureTestRequest,
    FixtureTestResponse,
    GcRequest,
    GcResponse,
    ResetRequest,
    ResetResponse,
    RunChecksRequest,
    RunChecksResponse,
)
from interlace.state.locks import hold_apply_lock


@get("/checks")
async def get_checks(
    state: State, model: FromQuery[str | None] = None, limit: FromQuery[int | None] = None
) -> list[CheckResultInfo]:
    rows = await state.store.list_check_results(model)
    return [CheckResultInfo(**row) for row in (rows[:limit] if limit else rows)]


@post("/tests/run", opt={"scope": "write"})
async def post_tests_run(state: State, data: FixtureTestRequest | None = None) -> FixtureTestResponse:
    """Build selected models in an ephemeral DuckDB and diff ``tests/golden``.

    Does not touch the warehouse, live checks, or the promotion gate.
    """
    from interlace.exceptions import PlanError
    from interlace.testing.golden import run_fixture_tests

    request = data or FixtureTestRequest()
    await reload_if_stale(state)
    compiled: CompiledProject = state.compiled
    try:
        chosen = select_models(request.selectors, compiled) if request.selectors else None
        report = await asyncio.to_thread(
            run_fixture_tests, compiled, Path(state.root), select=chosen, update=request.update_golden
        )
    except (SelectionError, PlanError) as exc:
        raise ClientException(detail=exc.message) from exc
    return FixtureTestResponse(ok=report.ok, passed=list(report.passed), messages=list(report.messages))


@post("/checks/run", opt={"scope": "write"})
async def post_checks_run(state: State, data: RunChecksRequest | None = None) -> RunChecksResponse:
    """Run checks ad hoc against an environment's promoted tables (dbt-test style),
    recording the results."""
    from interlace.checks.runner import run_promoted_checks
    from interlace.exceptions import PlanError

    request = data or RunChecksRequest()
    await reload_if_stale(state)
    env = request.environment or state.environment
    try:
        results, skipped = await run_promoted_checks(state.compiled, state.store, state.engines, env, request.selectors)
    except PlanError as exc:
        raise NotFoundException(detail=exc.message) from exc
    except SelectionError as exc:
        raise ClientException(detail=exc.message) from exc
    outcomes = [
        CheckOutcomeInfo(
            model=outcome.model,
            name=outcome.name,
            check_type=outcome.type,
            severity=outcome.severity,
            status=outcome.status,
            failures=outcome.failures,
            message=outcome.message,
        )
        for outcome in results
    ]
    return RunChecksResponse(
        environment=env,
        outcomes=outcomes,
        skipped=sorted(skipped),
        passed=sum(1 for outcome in outcomes if outcome.status == "passed"),
        blocking_failures=sum(1 for outcome in results if outcome.blocking),
    )


@get("/apikeys", opt={"scope": "admin"})
async def get_apikeys(state: State) -> list[ApiKeyInfo]:
    return [
        ApiKeyInfo(name=str(k["name"]), scopes=[str(s) for s in k["scopes"]], created_at=str(k["created_at"]))
        for k in await state.store.list_api_keys()
    ]


@post("/apikeys", opt={"scope": "admin"})
async def post_apikey(data: CreateApiKey, state: State) -> dict:
    from interlace.exceptions import ConfigurationError

    try:
        token = await state.store.create_api_key(data.name, data.scopes)
    except ConfigurationError as exc:
        raise ClientException(detail=exc.message) from exc
    return {"name": data.name.strip(), "scopes": data.scopes, "token": token}  # token shown once


@delete("/apikeys/{name:str}", opt={"scope": "admin"}, status_code=200)
async def delete_apikey(name: FromPath[str], state: State) -> dict:
    from interlace.exceptions import ConfigurationError

    try:
        removed = await state.store.revoke_api_key(name)
    except ConfigurationError as exc:
        raise ClientException(detail=exc.message) from exc
    if not removed:
        raise NotFoundException(detail=f"no key named {name!r}")
    return {"name": name, "removed": removed}


@post("/gc", opt={"scope": "admin"})
async def post_gc(state: State, data: GcRequest | None = None) -> GcResponse:
    """Garbage-collect unreferenced snapshots and their physical tables."""
    from interlace.state.interval import parse_grain
    from interlace.state.janitor import gc_project

    request = data or GcRequest()
    try:
        grace = parse_grain(request.grace)
    except ValueError as exc:
        raise ClientException(detail=str(exc)) from exc
    result, _, _swept = await gc_project(
        state.store,
        state.engines,
        grace=grace,
        dry_run=request.dry_run,
        lock_owner=state.lock_owner,
        streams=state.streams.values(),
        stream_log=state.stream_log,
    )
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


@post("/reset", opt={"scope": "admin"})
async def post_reset(state: State, data: ResetRequest | None = None) -> ResetResponse:
    """Wipe Interlace-owned state for a fresh apply. External table/file
    destinations are not dropped; terminal models stay recorded so the next
    apply will not re-deliver into them. Requires confirm=true (or dry_run)."""
    from interlace.state.janitor import reset as run_reset

    request = data or ResetRequest()
    if not request.dry_run and not request.confirm:
        raise ClientException(detail="pass confirm=true to reset (or dry_run=true to preview)")
    await reload_if_stale(state)
    keep = [model.name for model in state.compiled.models.values() if model.is_terminal]
    async with hold_apply_lock(state.store, owner=state.lock_owner):
        result = await run_reset(
            state.store,
            engines=state.engines,
            keep_models=keep,
            stream_log=state.stream_log,
            clear_streams=bool(state.streams),
            dry_run=request.dry_run,
        )
    if not request.dry_run:
        await state.store.append_event(
            "reset.finished",
            payload={
                "views": len(result.dropped_views),
                "schemas": result.dropped_schemas,
                "snapshots": result.cleared_snapshots,
                "kept_terminals": result.kept_terminals,
            },
        )
    return ResetResponse(
        dropped_views=result.dropped_views,
        dropped_schemas=result.dropped_schemas,
        cleared_snapshots=result.cleared_snapshots,
        kept_terminals=result.kept_terminals,
        environments=result.environments,
        stream_log_cleared=result.stream_log_cleared,
        dry_run=result.dry_run,
    )


@get("/events")
async def get_events(state: State, after: FromQuery[int] = 0) -> list[EventInfo]:
    return [EventInfo(**event) for event in await state.store.read_events(after)]


@get("/events/stream", opt={"no_compress": True, "query_token": True})
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
            # Comment frames flush headers immediately (EventSource onopen) and keep
            # idle proxies from dropping a quiet stream — the UI is SSE-only.
            yield ServerSentEventMessage(comment="ok", data=None)
            while True:
                backlog = await state.store.read_events(cursor)
                if not backlog:
                    break
                for event in backlog:
                    cursor = int(event["seq"])
                    yield ServerSentEventMessage(data=json.dumps(event), id=str(cursor))
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=15.0)
                except TimeoutError:
                    yield ServerSentEventMessage(comment="keepalive", data=None)
                    continue
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
