"""The ``interlace`` CLI: plan and apply against a project."""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from interlace.exceptions import CheckError, ConfigurationError, SelectionError
from interlace.graph.column_lineage import column_lineage
from interlace.graph.project import CompiledProject
from interlace.graph.selectors import select_models
from interlace.plan.apply import ApplyResult
from interlace.plan.apply import apply as apply_plan
from interlace.plan.differ import diff
from interlace.plan.plan import ChangeType, Plan
from interlace.plan.run import run_plan
from interlace.project import Project
from interlace.scaffold import scaffold_project
from interlace.scheduler.engine import TriggerEngine, build_triggers
from interlace.scheduler.worker import drain
from interlace.streaming import ensure_stream_tables

app = typer.Typer(no_args_is_help=True, add_completion=False, help="Python/SQL-first data platform.")
console = Console()


def _version_callback(value: bool) -> None:
    if value:
        from interlace import __version__

        console.print(f"interlace {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: bool = typer.Option(
        False, "--version", "-v", callback=_version_callback, is_eager=True, help="Show the version and exit."
    ),
) -> None:
    pass


_ENV = typer.Option("dev", "--env", "-e", help="Target data environment.")
_PATH = typer.Option(Path("."), "--path", "-p", help="Project root.")
_SELECT = typer.Option([], "--select", "-s", help="Model selectors: name, +name, name+, tag:x.")
_START = typer.Option("", "--start", help="Window start (ISO), for incremental models.")
_END = typer.Option("", "--end", help="Window end (ISO), for incremental models.")
_FORWARD_ONLY = typer.Option(
    False,
    "--forward-only",
    help="Modified history-keeping models (merge/full_merge/scd2/incremental) keep their existing "
    "table and history; the new logic applies going forward. Requires a shape-compatible change.",
)


def _render_checks(result: ApplyResult) -> None:
    if not result.checks:
        return
    passed = sum(1 for c in result.checks if c.status == "passed")
    warned = [c for c in result.checks if c.status != "passed"]
    line = f"Checks: {passed}/{len(result.checks)} passed"
    if warned:
        line += "; " + ", ".join(f"[yellow]{c.model}.{c.name} {c.status} ({c.severity})[/yellow]" for c in warned)
    console.print(line)


def _selection(compiled: CompiledProject, selectors: list[str]) -> set[str] | None:
    if not selectors:
        return None
    try:
        return select_models(selectors, compiled)
    except SelectionError as exc:
        console.print(f"[red]{exc.message}[/red]")
        raise typer.Exit(1) from exc


@app.command()
def init(
    path: Path = typer.Argument(Path("."), help="Directory to initialise."),
    name: str = typer.Option("", "--name", "-n", help="Project name (defaults to the directory name)."),
) -> None:
    """Scaffold a new interlace project."""
    try:
        written = scaffold_project(path, name or None)
    except ConfigurationError as exc:
        console.print(f"[red]{exc.message}[/red] ({exc.details.get('path', '')})")
        raise typer.Exit(1) from exc
    console.print(f"[green]Initialised interlace project in {path}[/green]")
    for written_path in written:
        console.print(f"  + {written_path}")
    console.print("\nNext: [bold]interlace apply --env dev[/bold]")


_FORWARD_ONLY = typer.Option(
    False,
    "--forward-only",
    help="Modified history-keeping models (merge/full_merge/scd2/incremental) keep their existing "
    "table and history; the new logic applies going forward. Requires a shape-compatible change.",
)


@app.command()
def plan(
    environment: str = _ENV, path: Path = _PATH, select: list[str] = _SELECT, forward_only: bool = _FORWARD_ONLY
) -> None:
    """Show what apply would change in an environment."""
    asyncio.run(_plan(environment, path, select, forward_only))


@app.command()
def apply(
    environment: str = _ENV, path: Path = _PATH, select: list[str] = _SELECT, forward_only: bool = _FORWARD_ONLY
) -> None:
    """Build changed models and promote the environment."""
    asyncio.run(_apply(environment, path, select, forward_only))


async def _plan(environment: str, path: Path, select: list[str], forward_only: bool = False) -> None:
    project = Project.load(path)
    compiled = project.compile()
    state = await project.open_state()
    try:
        _render(
            await diff(compiled, environment, state, select=_selection(compiled, select), forward_only=forward_only),
            environment,
        )
    finally:
        await state.close()


async def _apply(environment: str, path: Path, select: list[str], forward_only: bool = False) -> None:
    project = Project.load(path)
    compiled = project.compile()
    engines = project.open_engines()
    state = await project.open_state()
    try:
        # Stream-fed projects must build without the daemon ever having run:
        # declared stream tables are ensured (empty) so models reading them work.
        if project.streams:
            await ensure_stream_tables(project.streams, engines.get())
        plan_result = await diff(
            compiled, environment, state, select=_selection(compiled, select), forward_only=forward_only
        )
        _render(plan_result, environment)
        if plan_result.is_empty:
            return
        try:
            result = await apply_plan(
                plan_result, compiled=compiled, engines=engines, state=state, base_path=project.root
            )
        except CheckError as exc:
            console.print(f"[red]{exc.message}[/red]")
            raise typer.Exit(1) from exc
        _render_checks(result)
        console.print(
            f"[green]Built {len(result.built)} model(s); promoted {result.promoted} to '{environment}'.[/green]"
        )
    finally:
        await state.close()
        engines.close()


@app.command()
def run(
    environment: str = _ENV, path: Path = _PATH, select: list[str] = _SELECT, start: str = _START, end: str = _END
) -> None:
    """Force-build models and promote, ignoring change detection.

    For incremental_by_time models, --start/--end set the catchup window
    (default: the latest grain interval).
    """
    asyncio.run(_execute(environment, path, select, start, end, restate=False))


@app.command()
def restate(
    environment: str = _ENV, path: Path = _PATH, select: list[str] = _SELECT, start: str = _START, end: str = _END
) -> None:
    """Reprocess incremental models over a window, ignoring the ledger (vs run, which skips filled)."""
    asyncio.run(_execute(environment, path, select, start, end, restate=True))


async def _execute(environment: str, path: Path, select: list[str], start: str, end: str, *, restate: bool) -> None:
    window_start = datetime.fromisoformat(start) if start else None
    window_end = datetime.fromisoformat(end) if end else None
    project = Project.load(path)
    compiled = project.compile()
    engines = project.open_engines()
    state = await project.open_state()
    try:
        if project.streams:  # as in _apply: stream tables must exist daemon or not
            await ensure_stream_tables(project.streams, engines.get())
        plan_result = await run_plan(
            compiled,
            environment,
            state,
            start=window_start,
            end=window_end,
            select=_selection(compiled, select),
            restate=restate,
        )
        try:
            result = await apply_plan(
                plan_result, compiled=compiled, engines=engines, state=state, base_path=project.root
            )
        except CheckError as exc:
            console.print(f"[red]{exc.message}[/red]")
            raise typer.Exit(1) from exc
        _render_checks(result)
        verb = "Restated" if restate else "Ran"
        console.print(
            f"[green]{verb} {len(result.built)} model(s); promoted {result.promoted} to '{environment}'.[/green]"
        )
    finally:
        await state.close()
        engines.close()


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
    from interlace.state.janitor import gc as run_gc
    from interlace.streaming.materializer import sweep_streams

    project = Project.load(path)
    engines = project.open_engines()
    state = await project.open_state()
    try:
        result = await run_gc(state, engines=engines, grace=parse_grain(grace), dry_run=dry_run)
        verb = "Would remove" if dry_run else "Removed"
        console.print(
            f"{verb} {len(result.removed_snapshots)} snapshot(s), dropped {len(result.dropped_tables)} table(s); "
            f"{result.kept_snapshots} snapshot(s) kept."
        )
        for table in result.dropped_tables:
            console.print(f"  - {table}")
        if project.streams and not dry_run:
            log = await project.open_stream_log()
            try:
                swept = await sweep_streams(project.streams, log, engines.get())
            finally:
                await log.close()
            if swept:
                console.print("Stream retention: " + ", ".join(f"{k} -{v}" for k, v in swept.items()))
    finally:
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
    project = Project.load(path)
    compiled = project.compile()
    engines = project.open_engines()
    state = await project.open_state()
    trigger_engine = TriggerEngine(build_triggers(compiled), state)
    try:
        while True:
            await trigger_engine.tick(datetime.now())
            ran = await drain(state, compiled, engines=engines, environment=environment, base_path=project.root)
            if ran:
                console.print(f"[green]ran {ran} scheduled run(s) in '{environment}'[/green]")
            if once:
                break
            await asyncio.sleep(interval)
    finally:
        await state.close()
        engines.close()


@app.command()
def serve(
    environment: str = _ENV,
    path: Path = _PATH,
    host: str = typer.Option("127.0.0.1", "--host", help="Bind host."),
    port: int = typer.Option(8000, "--port", help="Bind port."),
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
) -> None:
    """Run the interlace daemon: HTTP API + scheduler in one process (requires the `service` extra).

    Use --no-scheduler for an API-only process (run `interlace scheduler` separately).
    """
    try:
        import uvicorn

        from interlace.service.app import create_app
    except ImportError as exc:
        console.print("[red]The HTTP API needs the 'service' extra: pip install 'interlace[service]'[/red]")
        raise typer.Exit(1) from exc
    token = quack_token
    if quack and not token:
        import secrets

        token = secrets.token_hex(8)
        console.print(f"[bold]quack[/bold] warehouse at [cyan]{quack}[/cyan] · token [yellow]{token}[/yellow]")
        console.print("Clients: set [bold]database: quack:...[/bold] and INTERLACE_QUACK_TOKEN in the environment.")
    uvicorn.run(
        create_app(
            path,
            environment,
            quack=quack or None,
            quack_token=token or None,
            scheduler=scheduler,
            scheduler_interval=interval,
        ),
        host=host,
        port=port,
    )


@app.command("list")
def list_models(path: Path = _PATH, select: list[str] = _SELECT) -> None:
    """List models with their materialisation, strategy, and dependencies."""
    project = Project.load(path)
    compiled = project.compile()
    chosen = _selection(compiled, select)
    table = Table(title="Models")
    table.add_column("Model")
    table.add_column("Output")
    table.add_column("Strategy")
    table.add_column("Depends on")
    for name in compiled.graph.topological_sort():
        if chosen is not None and name not in chosen:
            continue
        model = compiled.models[name]
        output = "sink" if model.export is not None else model.materialise
        table.add_row(name, output, model.strategy, ", ".join(model.dependencies) or "—")
    console.print(table)


@app.command()
def lineage(
    model: str = typer.Argument(..., help="Model name."),
    path: Path = _PATH,
    columns: bool = typer.Option(False, "--columns", "-c", help="Show column-level lineage."),
) -> None:
    """Show a model's lineage — table-level, or column-level with --columns."""
    project = Project.load(path)
    compiled = project.compile()
    if model not in compiled.models:
        console.print(f"[red]unknown model: {model}[/red]")
        raise typer.Exit(1)

    if columns:
        sources = column_lineage(compiled).get(model, {})
        console.print(f"[bold]{model}[/bold] columns")
        if not sources:
            console.print("  (column lineage unavailable)")
        for output, refs in sources.items():
            rendered = ", ".join(f"{table}.{column}" for table, column in refs) or "—"
            console.print(f"  {output} ← {rendered}")
        return

    upstream = sorted(compiled.graph.ancestors(model))
    downstream = sorted(compiled.graph.descendants(model))
    console.print(f"[bold]{model}[/bold]")
    console.print(f"  upstream:   {', '.join(upstream) or '—'}")
    console.print(f"  downstream: {', '.join(downstream) or '—'}")


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
        token = await state.create_api_key(name, scopes)
    finally:
        await state.close()
    console.print(f"[green]created API key '{name}' ({', '.join(scopes)})[/green]")
    console.print(f"  {token}")
    console.print("[yellow]store it now — it will not be shown again[/yellow]")


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
    table = Table(title="API keys")
    table.add_column("Name")
    table.add_column("Scopes")
    table.add_column("Created")
    for key in keys:
        table.add_row(str(key["name"]), ", ".join(key["scopes"]), str(key["created_at"]))  # type: ignore[arg-type]
    console.print(table)


def _render(plan: Plan, environment: str) -> None:
    if plan.is_empty:
        console.print(f"No changes for [bold]{environment}[/bold].")
        return
    reused = {snapshot.name for snapshot in plan.reuses}
    table = Table(title=f"Plan · {environment}")
    table.add_column("Model")
    table.add_column("Change")
    table.add_column("Category")
    table.add_column("Build")
    for change in plan.changes:
        build = "reuse" if change.name in reused else ("—" if change.change_type is ChangeType.REMOVED else "rebuild")
        table.add_row(change.name, change.change_type.value, change.category.value if change.category else "—", build)
    console.print(table)
    if reused:
        console.print(f"[dim]{len(reused)} model(s) have provably identical output — reusing existing tables.[/dim]")
    for transfer in plan.transfers:
        console.print(
            f"[cyan]transfer[/cyan] {transfer.model}: {transfer.source.name} → {transfer.target.name} "
            f"({transfer.via} → {transfer.table.schema}.{transfer.table.name})"
        )


def main() -> None:
    app()
