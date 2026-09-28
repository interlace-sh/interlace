"""The interlace HTTP API (Litestar).

A read + trigger surface over a project. Route handlers live in
``service.routes``; this module opens the project, the warehouse, and the
control-plane store for the process lifetime and registers those handlers.
"""

from __future__ import annotations

from pathlib import Path

from litestar import Litestar, Request
from litestar.config.compression import CompressionConfig
from litestar.datastructures import CacheControlHeader
from litestar.openapi import OpenAPIConfig
from litestar.openapi.plugins import ScalarRenderPlugin
from litestar.static_files import create_static_files_router

from interlace import __version__
from interlace.exceptions import BreakingPlanError, LockError
from interlace.service.auth import auth_guard
from interlace.service.lifespan import project_lifespan
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

    lifespan = project_lifespan(
        root,
        environment,
        quack=quack,
        quack_token=quack_token,
        scheduler=scheduler,
        scheduler_interval=scheduler_interval,
        stream_flush_interval=stream_flush_interval,
    )

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
