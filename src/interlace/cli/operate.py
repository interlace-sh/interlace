"""Serve, schedule, environments, API keys, gc, and reset."""

from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path
from typing import Any

import typer
from rich.markup import escape

from interlace.cli.support import (
    _ENV,
    _JSON,
    _PATH,
    _emit_json,
    _table,
    app,
    console,
)
from interlace.exceptions import (
    ConfigurationError,
    LockError,
    PlanError,
)
from interlace.project import Project
from interlace.state.locks import hold_apply_lock
from interlace.streaming import ensure_stream_tables


@app.command()
def gc(
    path: Path = _PATH,
    grace: str = typer.Option("7d", "--grace", help="Keep unreferenced snapshots younger than this (e.g. 7d, 12h)."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report what would be removed without touching anything."),
) -> None:
    """Garbage-collect snapshots no environment references, and their physical tables."""
    asyncio.run(_gc(path, grace, dry_run))


async def _gc(path: Path, grace: str, dry_run: bool) -> None:
    from interlace.state.interval import parse_grain
    from interlace.state.janitor import gc_project

    try:
        parsed_grace = parse_grain(grace)
    except ValueError as exc:
        console.print(f"[red]--grace must be a grain like 7d or 12h; got {grace!r}[/red]")
        raise typer.Exit(2) from exc
    project = Project.load(path)
    engines = project.open_engines()
    state = await project.open_state()
    log = await project.open_stream_log() if project.streams and not dry_run else None
    try:
        result, trimmed, swept = await gc_project(
            state,
            engines,
            grace=parsed_grace,
            dry_run=dry_run,
            lock_owner=f"cli:{os.getpid()}:gc",
            streams=project.streams,
            stream_log=log,
        )
        verb = "Would remove" if dry_run else "Removed"
        console.print(
            f"{verb} {len(result.removed_snapshots)} snapshot(s), dropped {len(result.dropped_tables)} table(s); "
            f"{result.kept_snapshots} snapshot(s) kept."
        )
        if any(trimmed.values()):
            console.print(
                f"Trimmed {trimmed['events']} event(s), {trimmed['check_results']} check result(s), "
                f"{trimmed['runs']} finished run(s) older than 30 days."
            )
        for table in result.dropped_tables:
            console.print(f"  - {table}")
        if swept:
            console.print("Stream retention: " + ", ".join(f"{k} -{v}" for k, v in swept.items()))
    finally:
        if log is not None:
            await log.close()
        await state.close()
        engines.close()


@app.command()
def reset(
    path: Path = _PATH,
    yes: bool = typer.Option(False, "--yes", help="Required. Confirm the reset."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report what would be removed without touching anything."),
    as_json: bool = _JSON,
) -> None:
    """Wipe Interlace-owned state for a fresh apply.

    Drops environment views, snapshot tables, runs, events, and the stream log.
    External table/file destinations are not dropped; terminal models stay
    recorded so the next apply will not re-deliver into them. API keys are kept.
    """
    asyncio.run(_reset(path, yes, dry_run, as_json))


async def _reset(path: Path, yes: bool, dry_run: bool, as_json: bool) -> None:
    from dataclasses import asdict

    from interlace.state.janitor import reset as run_reset

    if not yes and not dry_run:
        console.print(
            "This wipes Interlace-owned views, snapshot tables, runs, and stream state. "
            "External table/file destinations are not dropped."
        )
        console.print("Pass [bold]--yes[/bold] to proceed (or [bold]--dry-run[/bold] to preview).")
        raise typer.Exit(1)
    project = Project.load(path)
    compiled = project.compile()
    keep = [model.name for model in compiled.models.values() if model.is_terminal]
    engines = project.open_engines()
    state = await project.open_state()
    stream_log = None
    stream_path = project.root / project.config.stream_path
    if project.streams or stream_path.exists():
        stream_log = await project.open_stream_log()
    try:
        async with hold_apply_lock(state, owner=f"cli:{os.getpid()}:reset"):
            result = await run_reset(
                state,
                engines=engines,
                keep_models=keep,
                stream_log=stream_log,
                clear_streams=bool(project.streams),
                dry_run=dry_run,
            )
        if as_json:
            _emit_json(asdict(result))
            return
        verb = "Would drop" if dry_run else "Dropped"
        console.print(
            f"{verb} {len(result.dropped_views)} view(s), {len(result.dropped_schemas)} schema(s); "
            f"{'would clear' if dry_run else 'cleared'} {result.cleared_snapshots} snapshot(s)."
        )
        if result.kept_terminals:
            console.print(
                f"[dim]Left {len(result.kept_terminals)} terminal model(s) recorded "
                f"(table/file destinations were not dropped).[/dim]"
            )
        if result.stream_log_cleared:
            console.print("[dim]Stream log cleared.[/dim]")
        if not dry_run:
            console.print("[dim]Next apply rebuilds owned models from scratch.[/dim]")
    finally:
        if stream_log is not None:
            await stream_log.close()
        await state.close()
        engines.close()


@app.command()
def scheduler(
    environment: str = _ENV,
    path: Path = _PATH,
    interval: float = typer.Option(60.0, "--interval", help="Seconds between scheduler ticks."),
    once: bool = typer.Option(False, "--once", help="Run a single tick + drain, then exit."),
) -> None:
    """Run the scheduler: tick triggers, enqueue due runs, and execute them."""
    asyncio.run(_scheduler(environment, path, interval, once))


async def _scheduler(environment: str, path: Path, interval: float, once: bool) -> None:
    """Same loops as ``interlace serve``: reload, tick, trim, drain, flush, CDC."""
    from types import SimpleNamespace

    from interlace.scheduler.daemon import (
        cdc_loop,
        flush_once,
        flusher_loop,
        remember_runtime,
        scheduler_loop,
        source_mtime,
    )
    from interlace.streaming.materializer import quarantine_stream, stream_consumers

    project = Project.load(path)
    compiled = project.compile()
    engines = project.open_engines()
    store = await project.open_state()
    streams = {stream.name: stream for stream in project.streams}
    stream_log = await project.open_stream_log() if streams else None
    shadows = [stream for stream in streams.values() if stream.on_schema_drift == "quarantine"]
    flush_targets = [*streams.values(), *(quarantine_stream(stream) for stream in shadows)]
    if streams:
        await ensure_stream_tables(flush_targets, engines.get())
    host = SimpleNamespace(
        project=project,
        compiled=compiled,
        store=store,
        engines=engines,
        engine=engines.get(),
        environment=environment,
        root=project.root,
        model_paths=project.config.model_paths,
        reload_lock=asyncio.Lock(),
        source_mtime=source_mtime(project.root, project.config.model_paths),
        lock_owner=f"cli:{os.getpid()}:scheduler",
        drain_wanted=asyncio.Event(),
        flush_wanted=asyncio.Event(),
        flush_dirty={target.name for target in flush_targets},
        flush_targets=flush_targets,
        streams=streams,
        stream_log=stream_log,
        log_heads=await stream_log.heads() if stream_log is not None else {},
        lineage={},
        describe_cache={},
        cdc=project.config.cdc,
        connections=project.config.connections,
        stream_consumer_map={name: sorted(stream_consumers(compiled, name)) for name in streams},
    )
    host.flushed_heads = dict(host.log_heads)
    remember_runtime(host, project.config)
    flusher: asyncio.Task[None] | None = None
    cdc: asyncio.Task[None] | None = None
    try:
        if streams:
            await flush_once(host)  # this tick's models read rows that are already durable
            flusher = asyncio.create_task(flusher_loop(host, flush_interval=0.05))
        if project.config.cdc:
            cdc = asyncio.create_task(cdc_loop(host))

        def report(ran: int) -> None:
            console.print(f"[green]ran {ran} scheduled run(s) in '{environment}'[/green]")

        await scheduler_loop(host, interval=interval, once=once, on_ran=report)
    finally:
        for task in (cdc, flusher):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        if stream_log is not None:
            await stream_log.close()
        await store.close()
        engines.close()


async def _has_api_keys(path: Path) -> bool:
    state = await Project.load(path).open_state()
    try:
        return await state.count_api_keys() > 0
    finally:
        await state.close()


def _free_port(host: str, start: int, attempts: int = 50) -> int:
    """The requested port, or the next free one above it. A small probe/bind race
    remains possible; uvicorn still fails loudly if it loses it."""
    import errno
    import socket

    for candidate in range(start, start + attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind((host, candidate))
                return candidate
            except OSError as exc:
                if exc.errno not in (errno.EADDRINUSE, errno.EACCES):
                    raise
    console.print(f"[red]no free port in {start}–{start + attempts - 1}[/red]")
    raise typer.Exit(1)


@app.command()
def serve(
    environment: str = _ENV,
    path: Path = _PATH,
    host: str = typer.Option("127.0.0.1", "--host", help="Bind host."),
    port: int = typer.Option(8000, "--port", help="Bind port (if busy, the next free port is used)."),
    quack: str = typer.Option(
        "", "--quack", help="Also serve the warehouse over the quack protocol, e.g. quack:localhost:4213."
    ),
    quack_token: str = typer.Option(
        "", "--quack-token", help="Auth token for --quack (default: generated and printed)."
    ),
    scheduler: bool = typer.Option(
        True, "--scheduler/--no-scheduler", help="Run the scheduler loop in this process (combined daemon)."
    ),
    interval: float = typer.Option(60.0, "--interval", help="Seconds between scheduler ticks."),
    apply_on_start: bool = typer.Option(
        True,
        "--apply/--no-apply",
        help="Apply the project once at startup. --no-apply serves the warehouse as it is.",
    ),
    allow_open: bool = typer.Option(
        False,
        "--allow-open",
        help="Permit a non-loopback bind with no API keys (insecure). Refused without this flag.",
    ),
) -> None:
    """Run the interlace daemon: HTTP API + scheduler in one process (requires the `service` extra).

    Use --no-scheduler for an API-only process (run `interlace scheduler` separately).
    The project is applied once before the loops start; pass --no-apply to skip that.
    A `cdc:` block in the project config is read here: each Postgres slot appends into its stream.
    """
    try:
        import uvicorn

        from interlace.service.app import create_app
    except ImportError as exc:
        console.print(r"[red]The HTTP API needs the 'service' extra: pip install 'interlaced\[service]'[/red]")
        raise typer.Exit(1) from exc
    bound = _free_port(host, port)
    if bound != port:
        console.print(f"[yellow]port {port} is in use — serving on {bound}[/yellow]")
    port = bound
    # Auth is open until the first API key exists (see auth.py). Fine on loopback;
    # on a routable bind it exposes apply/gc/query unauthenticated — refuse unless
    # --allow-open (or keys already exist).
    if host not in ("127.0.0.1", "localhost", "::1"):
        keyed = asyncio.run(_has_api_keys(path))
        if not keyed and not allow_open:
            console.print(
                f"[bold red]refusing[/bold red] to serve on [bold]{host}[/bold] with no API keys — "
                "create one ([bold]interlace apikey create <name> --scope admin[/bold]) or pass "
                "[bold]--allow-open[/bold] (insecure)."
            )
            raise typer.Exit(1)
        if not keyed:
            console.print(
                f"[bold red]WARNING[/bold red] serving on [bold]{host}[/bold] with no API keys — "
                "the API is open to the network (--allow-open)."
            )
    console.print(f"UI at [bold cyan]http://{host}:{port}/ui[/bold cyan]")
    token = quack_token
    if quack and not token:
        import secrets

        token = secrets.token_hex(8)
        console.print(f"[bold]quack[/bold] warehouse at [cyan]{quack}[/cyan] · token [yellow]{token}[/yellow]")
        console.print("Clients: set [bold]database: quack:...[/bold] and INTERLACE_QUACK_TOKEN in the environment.")
    app = create_app(
        path,
        environment,
        quack=quack or None,
        quack_token=token or None,
        scheduler=scheduler,
        scheduler_interval=interval,
        apply_on_start=apply_on_start,
    )
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        # SSE clients (/events/stream) hold their response open forever. The app's
        # shutdown watcher ends them the instant this server flips `should_exit`, so
        # the graceful-shutdown drain finds the connections already closed instead of
        # force-cancelling them (which used to dump a CancelledError traceback on
        # Ctrl+C). The bound is a backstop; lifespan cleanup runs after the drain.
        timeout_graceful_shutdown=3,
    )
    server = uvicorn.Server(config)
    app.state.uvicorn_server = server  # let the app release SSE streams as shutdown begins
    server.run()


@app.command()
def mcp(path: Path = _PATH) -> None:
    """Serve this project to an MCP client on stdio.

    Tools list models, preview rows, plan, apply, query, lineage, checks, and runs.
    ``apply`` does nothing unless the client passes ``confirm: true`` after reading
    a plan. stdout is the protocol; logs stay on stderr.
    """
    from interlace.mcp_server import serve_stdio

    serve_stdio(path)


env_app = typer.Typer(no_args_is_help=True, help="Inspect and manage environments.")
app.add_typer(env_app, name="env")


@env_app.command("list")
def env_list(path: Path = _PATH, as_json: bool = _JSON) -> None:
    """List environments: promoted models and drift against the compiled project."""
    asyncio.run(_envs(path, as_json))


@env_app.command("drop")
def env_drop(
    name: str = typer.Argument(..., help="Environment to remove."),
    path: Path = _PATH,
    force: bool = typer.Option(False, "--force", help="Required to drop the production environment."),
) -> None:
    """Drop an environment: its views go, its snapshots become reclaimable by gc."""
    asyncio.run(_env_drop(name, path, force))


async def _env_drop(name: str, path: Path, force: bool) -> None:
    from interlace.plan.plan import PRODUCTION_ENV
    from interlace.state.janitor import drop_environment

    if name == PRODUCTION_ENV and not force:
        console.print(f"[red]{name!r} is the production environment (unprefixed views); pass --force to drop it.[/red]")
        raise typer.Exit(1)
    project = Project.load(path)
    engines = project.open_engines()
    state = await project.open_state()
    try:
        if not await state.get_environment(name):
            console.print(f"No environment {name!r}.")
            raise typer.Exit(1)
        async with hold_apply_lock(state, owner=f"cli:{os.getpid()}:env-drop"):
            dropped = await drop_environment(state, engines=engines, environment=name)
        console.print(f"Dropped environment [bold]{name}[/bold] ({len(dropped)} view(s) removed).")
        console.print("[dim]Its snapshots are now unreferenced — `interlace gc` reclaims their tables.[/dim]")
    finally:
        await state.close()
        engines.close()


@env_app.command("rollback")
def env_rollback(
    name: str = typer.Argument("prod", help="Environment to roll back."),
    path: Path = _PATH,
    to: int = typer.Option(0, "--to", help="Target generation (0 = the one before the latest)."),
    history: bool = typer.Option(False, "--list", help="Show the promotion history instead of rolling back."),
    as_json: bool = _JSON,
) -> None:
    """Repoint an environment's views at an earlier promotion — the marquee benefit
    of fingerprinted snapshots: nothing rebuilds, views move."""
    asyncio.run(_env_rollback(name, path, to or None, history, as_json))


async def _env_rollback(name: str, path: Path, to: int | None, history: bool, as_json: bool) -> None:
    from interlace.state.janitor import rollback_environment

    project = Project.load(path)
    state = await project.open_state()
    engines = None
    try:
        if history:
            generations = await state.list_generations(name)
            if as_json:
                _emit_json(generations)
                return
            if not generations:
                console.print(f"No promotion history for {name!r}.")
                return
            table = _table(f"Promotions — {name}")
            table.add_column("Generation", justify="right")
            table.add_column("Promoted", style="dim")
            table.add_column("Models", justify="right")
            for row in generations:
                marker = " (current)" if row is generations[0] else ""
                table.add_row(f"{row['generation']}{marker}", str(row["promoted_at"]), str(row["models"]))
            console.print(table)
            return
        engines = project.open_engines()
        try:
            async with hold_apply_lock(state, owner=f"cli:{os.getpid()}:env-rollback"):
                result = await rollback_environment(state, engines=engines, environment=name, to_generation=to)
        except PlanError as exc:
            console.print(f"[red]{escape(exc.message)}[/red]")
            raise typer.Exit(1) from exc
        except LockError as exc:
            console.print(f"[red]{escape(exc.message)}[/red]")
            raise typer.Exit(1) from exc
        if as_json:
            _emit_json(result)
            return
        console.print(
            f"Rolled [bold]{name}[/bold] back to generation {result['generation']}: "
            f"{len(result['repointed'])} view(s) repointed"  # type: ignore[arg-type]
            + (f", {len(result['removed_views'])} removed" if result["removed_views"] else "")  # type: ignore[arg-type]
            + "."
        )
        console.print("[dim]Nothing was rebuilt — the views moved. Apply again to return to the latest state.[/dim]")
    finally:
        await state.close()
        if engines is not None:
            engines.close()


async def _envs(path: Path, as_json: bool = False) -> None:
    from interlace.plan.plan import PRODUCTION_ENV

    project = Project.load(path)
    compiled = project.compile()
    state = await project.open_state()
    try:
        names = await state.list_environments()
        rows: list[dict[str, Any]] = []
        for name in names:
            promoted = await state.get_environment(name)
            drift = sum(1 for m in compiled.models.values() if promoted.get(m.name) != m.fingerprint)
            views = "main.* (production)" if name == PRODUCTION_ENV else f"{name}__*.*"
            rows.append({"name": name, "views": views, "models": len(promoted), "drift": drift})
        if as_json:
            _emit_json(rows)
            return
        if not names:
            console.print("No environments promoted yet — run [bold]interlace apply[/bold].")
            return
        table = _table("Environments")
        table.add_column("Environment")
        table.add_column("Views", style="dim")
        table.add_column("Models", justify="right")
        table.add_column("Drift", justify="right")
        for row in rows:
            drift_cell = f"[yellow]{row['drift']}[/]" if row["drift"] else "[dim]—[/]"
            table.add_row(str(row["name"]), str(row["views"]), str(row["models"]), drift_cell)
        console.print(table)
    finally:
        await state.close()


apikey_app = typer.Typer(no_args_is_help=True, help="Manage HTTP API keys.")
app.add_typer(apikey_app, name="apikey")


@apikey_app.command("create")
def apikey_create(
    name: str = typer.Argument(..., help="A label for the key."),
    path: Path = _PATH,
    scope: list[str] = typer.Option(["read"], "--scope", help="Scopes: read, write, admin."),
) -> None:
    """Create an API key and print it once."""
    asyncio.run(_apikey_create(name, path, scope))


async def _apikey_create(name: str, path: Path, scopes: list[str]) -> None:

    state = await Project.load(path).open_state()
    try:
        try:
            token = await state.create_api_key(name, scopes)
        except ConfigurationError as exc:
            console.print(f"[red]{escape(exc.message)}[/red]")
            raise typer.Exit(1) from exc
    finally:
        await state.close()
    console.print(f"[green]created API key '{name}' ({', '.join(scopes)})[/green]")
    console.print(f"  {token}")
    console.print("[yellow]store it now — it will not be shown again[/yellow]")


@apikey_app.command("revoke")
def apikey_revoke(
    name: str = typer.Argument(..., help="The key name to revoke (every key with this name)."),
    path: Path = _PATH,
) -> None:
    """Revoke an API key — it stops authenticating immediately."""
    asyncio.run(_apikey_revoke(name, path))


async def _apikey_revoke(name: str, path: Path) -> None:

    state = await Project.load(path).open_state()
    try:
        try:
            removed = await state.revoke_api_key(name)
        except ConfigurationError as exc:
            console.print(f"[red]{escape(exc.message)}[/red]")
            raise typer.Exit(1) from exc
    finally:
        await state.close()
    if removed:
        console.print(f"[green]revoked {removed} key(s) named '{name}'[/green]")
    else:
        console.print(f"[yellow]no key named '{name}'[/yellow]")
        raise typer.Exit(1)


@apikey_app.command("list")
def apikey_list(path: Path = _PATH) -> None:
    """List API keys (names and scopes, not the secrets)."""
    asyncio.run(_apikey_list(path))


async def _apikey_list(path: Path) -> None:
    state = await Project.load(path).open_state()
    try:
        keys = await state.list_api_keys()
    finally:
        await state.close()
    table = _table("API keys")
    table.add_column("Name")
    table.add_column("Scopes")
    table.add_column("Created", style="dim")
    for key in keys:
        table.add_row(str(key["name"]), ", ".join(key["scopes"]), str(key["created_at"]))  # type: ignore[arg-type]
    console.print(table)
