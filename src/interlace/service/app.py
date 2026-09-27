"""The interlace HTTP API (Litestar).

A read + trigger surface over a project. Route handlers live in
``service.routes``; this module opens the project, the warehouse, and the
control-plane store for the process lifetime and registers those handlers.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from litestar import Litestar, Request
from litestar.config.compression import CompressionConfig
from litestar.datastructures import CacheControlHeader
from litestar.exceptions import ImproperlyConfiguredException
from litestar.openapi import OpenAPIConfig
from litestar.openapi.plugins import ScalarRenderPlugin
from litestar.static_files import create_static_files_router

from interlace import __version__
from interlace.exceptions import BreakingPlanError, LockError
from interlace.graph.column_lineage import column_lineage
from interlace.project import Project
from interlace.scheduler.daemon import (
    cdc_loop,
    flush_once,
    flusher_loop,
    scheduler_loop,
    source_mtime,
)
from interlace.service.auth import auth_guard
from interlace.service.present import _broadcast
from interlace.service.routes.admin import (
    delete_apikey,
    get_apikeys,
    get_checks,
    get_events,
    post_apikey,
    post_checks_run,
    post_gc,
    post_reset,
    post_tests_run,
    stream_events,
)
from interlace.service.routes.catalog import (
    get_connections,
    get_engines,
    get_lineage,
    get_schedules,
    post_query,
)
from interlace.service.routes.environments import (
    drop_environment_endpoint,
    get_environment_history,
    get_environments,
    rollback_environment_endpoint,
)
from interlace.service.routes.models import (
    get_check_rows,
    get_model,
    get_model_impact,
    get_model_preview,
    get_models,
    health,
    ui_redirect,
)
from interlace.service.routes.plan import (
    get_plan,
    post_apply,
    post_run,
)
from interlace.service.routes.runs import (
    cancel_run,
    create_run,
    get_run,
    get_runs,
    post_hook,
)
from interlace.service.routes.streams import (
    commit_stream,
    get_stream,
    get_streams,
    publish,
    stream_log_events,
)
from interlace.streaming.materializer import (
    ensure_stream_tables,
    quarantine_stream,
    stream_consumers,
)


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

    # The UI is self-contained and air-gapped: a CSP locks every fetch to same-origin,
    # enforcing (not just documenting) that mandate. style-src allows 'unsafe-inline'
    # because the views set element style="…" attributes; scripts are external ES
    # modules only, so script-src stays 'self' with no inline-exec escape hatch. Scoped
    # to /ui by router middleware, so the Scalar API docs and JSON endpoints are untouched.
    from litestar.types import ASGIApp, Message, Receive, Scope, Send

    ui_csp = (
        "default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; "
        "img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; "
        "connect-src 'self'; font-src 'self'; form-action 'self'"
    )

    def ui_security_headers(app: ASGIApp) -> ASGIApp:  # Litestar calls the factory as app=<next>
        async def wrapped(scope: Scope, receive: Receive, send: Send) -> None:
            if scope["type"] != "http":
                await app(scope, receive, send)
                return

            async def send_wrapper(message: Message) -> None:
                if message["type"] == "http.response.start":
                    # ASGI types `headers` as an Iterable, not a list, so copy into one
                    # rather than appending to whatever the server happened to pass.
                    message["headers"] = [
                        *message.get("headers", []),
                        (b"content-security-policy", ui_csp.encode()),
                        (b"x-content-type-options", b"nosniff"),
                        (b"x-frame-options", b"DENY"),
                    ]
                await send(message)

            await app(scope, receive, send_wrapper)

        return wrapped

    # no-cache (not no-store): the browser may keep copies but must revalidate,
    # so upgrading the daemon can never serve a stale shell against new modules
    ui_router = create_static_files_router(
        path="/ui",
        directories=[Path(__file__).parent / "ui"],
        html_mode=True,
        include_in_schema=False,
        cache_control=CacheControlHeader(no_cache=True),
        middleware=[ui_security_headers],
    )
    from litestar import Response

    from interlace.exceptions import InterlaceError as _InterlaceError

    def _domain_error(request: Request, exc: Exception) -> Response:
        # user-caused errors (bad selector, unknown engine, contract violation)
        # are 4xx with their message — never anonymous 500s
        message = getattr(exc, "message", str(exc))
        body: dict[str, object] = {"detail": message}
        details = getattr(exc, "details", None)
        statement = details.get("statement") if isinstance(details, dict) else None
        if isinstance(statement, str) and statement:
            body["statement"] = statement
        if isinstance(exc, LockError | BreakingPlanError):
            return Response(content=body, status_code=409)
        status = 404 if "unknown" in message[:40].lower() else 400
        return Response(content=body, status_code=status)

    return Litestar(
        exception_handlers={_InterlaceError: _domain_error},
        # gzip the served modules/CSS/JSON (no build step, so this is where transfer
        # size is won). SSE routes set no_compress — compressing them would buffer
        # and stall live events. minimum_size skips tiny bodies where framing costs more.
        compression_config=CompressionConfig(backend="gzip", exclude_opt_key="no_compress"),
        route_handlers=[
            ui_router,
            ui_redirect,
            health,
            get_models,
            get_model,
            get_model_impact,
            get_model_preview,
            get_check_rows,
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
            post_run,
            get_checks,
            post_checks_run,
            get_streams,
            get_stream,
            publish,
            stream_log_events,
            commit_stream,
            post_gc,
            post_reset,
            post_query,
            get_engines,
            get_connections,
            get_schedules,
            post_hook,
            post_tests_run,
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
