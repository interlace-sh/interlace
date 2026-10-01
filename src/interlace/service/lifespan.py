"""Process lifetime for ``interlace serve``.

Opens the project, warehouse, control plane, and stream log, then supervises
the event tail, flusher, scheduler, and CDC loop. Route registration stays in
``app.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from typing import cast

from litestar import Litestar
from litestar.exceptions import ImproperlyConfiguredException

from interlace.graph.column_lineage import column_lineage
from interlace.project import Project
from interlace.scheduler.daemon import (
    cdc_loop,
    flush_once,
    flusher_loop,
    remember_runtime,
    scheduler_loop,
    source_mtime,
    startup_apply,
)
from interlace.service.present import _broadcast
from interlace.streaming.materializer import (
    ensure_stream_tables,
    quarantine_stream,
    stream_consumers,
)


def project_lifespan(  # noqa: C901
    root: Path | str,
    environment: str,
    *,
    quack: str | None,
    quack_token: str | None,
    scheduler: bool,
    scheduler_interval: float,
    stream_flush_interval: float,
    apply_on_start: bool,
) -> Callable[[Litestar], AbstractAsyncContextManager[None, bool | None]]:
    """The Litestar lifespan for one project. Background loops retry; shutdown drains."""

    @asynccontextmanager
    async def lifespan(app: Litestar) -> AsyncIterator[None]:  # noqa: C901

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
        app.state.project = project
        app.state.compiled = compiled
        app.state.lineage = column_lineage(compiled)  # whole-project qualify: compute once, not per request
        app.state.store = store
        app.state.engine = engine
        app.state.engines = engines
        app.state.environment = environment
        app.state.root = project.root
        app.state.model_paths = project.config.model_paths  # for the on-demand recompile staleness probe
        app.state.reload_lock = asyncio.Lock()  # serialise recompiles; recompile at most once per change
        app.state.source_mtime = source_mtime(project.root, project.config.model_paths)
        remember_runtime(app.state, project.config)
        app.state.parallelism = project.config.parallelism
        app.state.engine_configs = project.config.engine_configs()
        app.state.connections = project.config.connections
        app.state.cdc = project.config.cdc
        app.state.default_engine = project.config.default_engine
        app.state.describe_cache = {}  # (model, fingerprint) -> {column: type}, filled by /lineage
        app.state.lock_owner = f"serve:{os.getpid()}"  # cross-process apply lock identity
        # Wake the scheduler's drain the instant a run is enqueued, instead of waiting
        # out the tick interval — an enqueue from the UI/API/stream picks up promptly.
        app.state.drain_wanted = asyncio.Event()
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

        if streams:
            app.state.flush_wanted.set()  # catch up anything durable but unflushed at last shutdown

        async def shutdown_watch() -> None:
            """End every open SSE stream the moment uvicorn begins shutting down.

            SSE responses (/events/stream) block forever on their queue, so at Ctrl+C
            uvicorn's graceful-shutdown drain waits the full timeout and then force-
            cancels them — which surfaces as a CancelledError traceback from the held-
            open stream. Poisoning the subscribers (their tail() returns on ``None``)
            lets the drain find the connections already closed, so shutdown is clean
            and immediate. ``uvicorn_server`` is injected by the serve command."""
            server = getattr(app.state, "uvicorn_server", None)
            if server is None:
                return
            while not getattr(server, "should_exit", False):
                await asyncio.sleep(0.2)
            for subscriber in list(app.state.sse_subscribers):
                with contextlib.suppress(asyncio.QueueFull):
                    subscriber.put_nowait(None)

        if apply_on_start:
            await startup_apply(app.state)

        tail_task = asyncio.create_task(event_tail())
        flusher_task = (
            asyncio.create_task(flusher_loop(app.state, flush_interval=stream_flush_interval)) if streams else None
        )
        loop_task = asyncio.create_task(scheduler_loop(app.state, interval=scheduler_interval)) if scheduler else None
        cdc_task = asyncio.create_task(cdc_loop(app.state)) if project.config.cdc else None
        watch_task = asyncio.create_task(shutdown_watch())
        try:
            yield
        finally:
            # Teardown must survive being CANCELLED (a second Ctrl+C, or uvicorn's
            # graceful-shutdown timeout, lands mid-teardown): anyio cancel scopes are
            # level-triggered, so every unshielded await below would raise instantly
            # and the store/log/engine handles would leak — shield the whole thing.
            import anyio

            # Belt-and-braces: release any SSE stream still open (the watcher above
            # normally does this the instant should_exit flips).
            for subscriber in list(app.state.sse_subscribers):
                with contextlib.suppress(asyncio.QueueFull):
                    subscriber.put_nowait(None)
            for task in (cdc_task, loop_task, flusher_task, tail_task, watch_task):
                if task is not None:
                    task.cancel()
                    # suppress Exception too: a task that already died must not
                    # abort teardown and leak the store/log/engine handles
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
            if streams:  # clean shutdown leaves nothing durable-but-unflushed behind
                with anyio.move_on_after(8, shield=True):  # best-effort, bounded: force-quit must still quit
                    with contextlib.suppress(Exception):
                        await flush_once(app.state)  # incl. consumer enqueues, so restarts owe nothing
            with anyio.CancelScope(shield=True):  # closes are fast and MUST run
                with contextlib.suppress(Exception):
                    await stream_log.close()
                with contextlib.suppress(Exception):
                    await store.close()
            engines.close()

    return cast("Callable[[Litestar], AbstractAsyncContextManager[None, bool | None]]", lifespan)
