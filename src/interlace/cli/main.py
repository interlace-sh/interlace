"""The ``interlace`` CLI: plan and apply against a project."""

from __future__ import annotations

import asyncio
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from interlace.plan.apply import apply as apply_plan
from interlace.plan.differ import diff
from interlace.plan.plan import Plan
from interlace.project import Project

app = typer.Typer(no_args_is_help=True, add_completion=False, help="Python/SQL-first data platform.")
console = Console()

_ENV = typer.Option("dev", "--env", "-e", help="Target data environment.")
_PATH = typer.Option(Path("."), "--path", "-p", help="Project root.")


@app.command()
def plan(environment: str = _ENV, path: Path = _PATH) -> None:
    """Show what apply would change in an environment."""
    asyncio.run(_plan(environment, path))


@app.command()
def apply(environment: str = _ENV, path: Path = _PATH) -> None:
    """Build changed models and promote the environment."""
    asyncio.run(_apply(environment, path))


async def _plan(environment: str, path: Path) -> None:
    project = Project.load(path)
    state = await project.open_state()
    try:
        _render(await diff(project.compile(), environment, state), environment)
    finally:
        await state.close()


async def _apply(environment: str, path: Path) -> None:
    project = Project.load(path)
    compiled = project.compile()
    engine = project.open_engine()
    state = await project.open_state()
    try:
        plan_result = await diff(compiled, environment, state)
        _render(plan_result, environment)
        if plan_result.is_empty:
            return
        result = await apply_plan(plan_result, compiled=compiled, engine=engine, state=state)
        console.print(
            f"[green]Built {len(result.built)} model(s); promoted {result.promoted} to '{environment}'.[/green]"
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
