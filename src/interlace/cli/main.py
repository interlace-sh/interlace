"""The ``interlace`` CLI: plan and apply against a project."""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from interlace.exceptions import ConfigurationError, SelectionError
from interlace.graph.project import CompiledProject
from interlace.graph.selectors import select_models
from interlace.plan.apply import apply as apply_plan
from interlace.plan.differ import diff
from interlace.plan.plan import Plan
from interlace.plan.run import run_plan
from interlace.project import Project
from interlace.scaffold import scaffold_project

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
