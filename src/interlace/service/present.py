"""Shared response builders for the HTTP routes."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable
from typing import Any

from litestar.datastructures import State
from litestar.exceptions import NotFoundException

from interlace.dsl.decorators import StreamDef
from interlace.graph.project import CompiledModel
from interlace.plan.apply import ApplyResult, ProgressCallback
from interlace.service.types import (
    ApplyResponse,
    BuildInfo,
    CheckOutcomeInfo,
    ModelInfo,
    ProfileColumn,
    SampleResponse,
)
from interlace.streaming.materializer import (
    flush_streams,
)

# The project is compiled once at startup, then recompiled on demand when a model
# file changes on disk — so editing a `.sql`/`.py` model and pressing Plan/Apply in
# the UI reflects the edit, matching what `interlace plan` (a fresh process) shows.
# Only the model graph is re-derived; changing engine/stream/path topology in
# interlace.yaml still needs a daemon restart.


def _event_progress(state: State, extra: dict[str, Any]) -> tuple[ProgressCallback, Callable[[], Awaitable[None]]]:
    """Fire-and-forget model.* events; the drain coroutine waits them out."""
    loop = asyncio.get_running_loop()
    tasks: set[asyncio.Task[None]] = set()

    def on_progress(model: str, event: str, detail: dict[str, Any]) -> None:
        payload: dict[str, Any] = {**extra, **detail}
        task = loop.create_task(state.store.append_event(f"model.{event}", entity=model, payload=payload))
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    async def drain() -> None:
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    return on_progress, drain


def _apply_response(env: str, result: ApplyResult | None, *, breaking: bool) -> ApplyResponse:
    if result is None:
        return ApplyResponse(environment=env, built=[], promoted=0, breaking=breaking)
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
        gated=result.gated,
        checks=[
            CheckOutcomeInfo(
                model=outcome.model,
                name=outcome.name,
                check_type=outcome.type,
                severity=outcome.severity,
                status=outcome.status,
                failures=outcome.failures,
                message=outcome.message,
            )
            for outcome in result.checks
        ],
    )


async def _flush_if_streams(state: State) -> None:
    if state.streams:
        await flush_streams(state.flush_targets, state.stream_log, state.engine)


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
    return model.materialise


def _info(model: CompiledModel, has_checks: bool = False) -> ModelInfo:
    return ModelInfo(
        name=model.name,
        output=_output(model),
        materialise=model.materialise,
        strategy=model.strategy,
        is_terminal=model.is_terminal,
        fingerprint=model.fingerprint,
        depends_on=list(model.dependencies),
        tags=list(model.tags),
        owner=model.owner,
        schedule=model.schedule,
        engine=model.engine,
        language="python" if model.ast is None else "sql",
        has_checks=has_checks,
    )


def _build_info(build: object) -> BuildInfo | None:
    from interlace.inspect import LastBuild

    if not isinstance(build, LastBuild):
        return None
    return BuildInfo(
        status=build.status,
        at=build.at,
        seconds=build.seconds,
        rows=build.rows,
        message=build.message,
        statement=build.statement,
    )


def _sample(preview: object, *, profile: bool) -> SampleResponse:
    from interlace.inspect import FailingRows, ModelPreview

    if isinstance(preview, ModelPreview):
        sample = preview.sample
        return SampleResponse(
            available=preview.available,
            message=preview.message,
            relation=preview.relation,
            columns=sample.columns,
            types=sample.types,
            rows=sample.rows,
            row_count=len(sample.rows),
            truncated=sample.truncated,
            profile=(
                [
                    ProfileColumn(
                        column=column.column,
                        type=column.type,
                        nulls=column.nulls,
                        distinct=column.distinct,
                        min=column.min,
                        max=column.max,
                    )
                    for column in preview.profile
                ]
                if profile
                else []
            ),
            last_build=_build_info(preview.last_build),
        )
    if isinstance(preview, FailingRows):
        sample = preview.sample
        return SampleResponse(
            available=preview.available,
            message=preview.message,
            columns=sample.columns,
            types=sample.types,
            rows=sample.rows,
            row_count=len(sample.rows),
            truncated=sample.truncated,
        )
    raise TypeError(type(preview).__name__)


def _stream_or_404(state: State, name: str) -> StreamDef:
    stream: StreamDef | None = state.streams.get(name)
    if stream is None:
        raise NotFoundException(detail=f"unknown stream: {name}")
    return stream


def _log_name_or_404(state: State, name: str) -> None:
    """A declared stream, or its ``<name>__quarantine`` shadow log."""
    if name in state.streams:
        return
    parent = name.removesuffix("__quarantine")
    if parent != name and parent in state.streams:
        return
    raise NotFoundException(detail=f"unknown stream: {name}")


def _jsonable(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)  # timestamps, decimals, structs — stringified for the wire


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
