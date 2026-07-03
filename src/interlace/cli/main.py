"""The ``interlace`` CLI: plan and apply against a project."""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from interlace.exceptions import ConfigurationError, SelectionError
from interlace.graph.column_lineage import column_lineage
from interlace.graph.project import CompiledProject
from interlace.graph.selectors import select_models
from interlace.plan.apply import apply as apply_plan
from interlace.plan.differ import diff
from interlace.plan.plan import Plan
from interlace.plan.run import run_plan
from interlace.project import Project
from interlace.scaffold import scaffold_project
from interlace.scheduler.engine import TriggerEngine, build_triggers
from interlace.scheduler.worker import drain

app = typer.Typer(no_args_is_help=True, add_completion=False, help="Python/SQL-first data platform.")
console = Console()

_ENV = typer.Option("dev", "--env", "-e", help="Target data environment.")
_PATH = typer.Option(Path("."), "--path", "-p", help="Project root.")
_SELECT = typer.Option([], "--select", "-s", help="Model selectors: name, +name, name+, tag:x.")
_START = typer.Option("", "--start", help="Window start (ISO), for incremental models.")
_END = typer.Option("", "--end", help="Window end (ISO), for incremental models.")


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


@app.command()
def plan(environment: str = _ENV, path: Path = _PATH, select: list[str] = _SELECT) -> None:
    """Show what apply would change in an environment."""
    asyncio.run(_plan(environment, path, select))


@app.command()
def apply(environment: str = _ENV, path: Path = _PATH, select: list[str] = _SELECT) -> None:
    """Build changed models and promote the environment."""
    asyncio.run(_apply(environment, path, select))


async def _plan(environment: str, path: Path, select: list[str]) -> None:
    project = Project.load(path)
    compiled = project.compile()
    state = await project.open_state()
    try:
        _render(await diff(compiled, environment, state, select=_selection(compiled, select)), environment)
    finally:
        await state.close()


async def _apply(environment: str, path: Path, select: list[str]) -> None:
    project = Project.load(path)
    compiled = project.compile()
    engine = project.open_engine()
    state = await project.open_state()
    try:
        plan_result = await diff(compiled, environment, state, select=_selection(compiled, select))
        _render(plan_result, environment)
        if plan_result.is_empty:
            return
        result = await apply_plan(plan_result, compiled=compiled, engine=engine, state=state, base_path=project.root)
        console.print(
            f"[green]Built {len(result.built)} model(s); promoted {result.promoted} to '{environment}'.[/green]"
        )
    finally:
        await state.close()
        engine.close()


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
    engine = project.open_engine()
    state = await project.open_state()
    try:
        plan_result = await run_plan(
            compiled,
            environment,
            state,
            start=window_start,
            end=window_end,
            select=_selection(compiled, select),
            restate=restate,
        )
        result = await apply_plan(plan_result, compiled=compiled, engine=engine, state=state, base_path=project.root)
        verb = "Restated" if restate else "Ran"
        console.print(
            f"[green]{verb} {len(result.built)} model(s); promoted {result.promoted} to '{environment}'.[/green]"
        )
    finally:
        await state.close()
        engine.close()


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
    engine = project.open_engine()
    state = await project.open_state()
    trigger_engine = TriggerEngine(build_triggers(compiled), state)
    try:
        while True:
            await trigger_engine.tick(datetime.now())
            ran = await drain(state, compiled, engine, environment, base_path=project.root)
            if ran:
                console.print(f"[green]ran {ran} scheduled run(s) in '{environment}'[/green]")
            if once:
                break
            await asyncio.sleep(interval)
    finally:
        await state.close()
        engine.close()


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
) -> None:
    """Run the HTTP API (requires the `service` extra). Run `interlace scheduler` to execute queued runs."""
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
    uvicorn.run(create_app(path, environment, quack=quack or None, quack_token=token or None), host=host, port=port)


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
    table = Table(title=f"Plan · {environment}")
    table.add_column("Model")
    table.add_column("Change")
    table.add_column("Category")
    for change in plan.changes:
        table.add_row(change.name, change.change_type.value, change.category.value if change.category else "—")
    console.print(table)


def main() -> None:
    app()
