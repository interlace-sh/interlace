"""Shared CLI application, options, and Rich rendering."""

from __future__ import annotations

import contextlib
from datetime import datetime
from pathlib import Path
from typing import Any

import typer
from rich import box
from rich.console import Console
from rich.markup import escape
from rich.progress import Progress, SpinnerColumn, TaskID, TextColumn, TimeElapsedColumn
from rich.table import Table

from interlace.exceptions import (
    InterlaceError,
    SelectionError,
)
from interlace.graph.project import CompiledProject
from interlace.graph.selectors import select_models
from interlace.plan.apply import ApplyResult
from interlace.plan.plan import ChangeType, Plan

# pretty_exceptions_enable=False: let InterlaceError propagate out of app() so main()
# can render it as one clean line instead of a Rich traceback through interlace internals.
# Unexpected (non-Interlace) errors still surface a normal traceback for debugging.
app = typer.Typer(no_args_is_help=True, help="Python/SQL-first data platform.", pretty_exceptions_enable=False)
console = Console()
err_console = Console(stderr=True)


class _BuildProgress:
    """Live per-model build rows: a row appears when a model starts, ✓/✗ when it ends.

    Doubles as the ``apply(on_progress=...)`` callback; use ``.progress`` as the
    context manager around the apply call.
    """

    def __init__(self) -> None:
        self.progress = Progress(
            SpinnerColumn(finished_text=" "),
            TextColumn("{task.description}"),
            TextColumn("{task.fields[status]}"),
            TimeElapsedColumn(),
            console=console,
        )
        self._rows: dict[str, TaskID] = {}

    def __call__(self, model: str, event: str, detail: dict[str, Any] | None = None) -> None:
        if event == "start":
            self._rows[model] = self.progress.add_task(model, total=1, status="")
            return
        task = self._rows.get(model)
        if task is None:  # a model cancelled by a sibling's failure before it ever started has no row
            return
        if event == "done":
            self.progress.update(task, completed=1, status="[green]✓[/green]")
        elif event == "cancelled":  # collateral of a sibling's failure, not a failure itself
            self.progress.update(task, completed=1, status="[dim]⊘[/dim]")
        else:  # failed
            self.progress.update(task, completed=1, status="[red]✗[/red]")


def _build_progress(plan_result: Plan) -> _BuildProgress | None:
    """Progress display when it can render live and there is something to watch."""
    return _BuildProgress() if console.is_terminal and plan_result.backfills else None


def _version_callback(value: bool) -> None:
    if value:
        from interlace import __version__

        console.print(f"interlace {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    ctx: typer.Context,
    version: bool = typer.Option(
        False, "--version", "-v", callback=_version_callback, is_eager=True, help="Show the version and exit."
    ),
) -> None:
    from interlace.state.store import event_actor

    # Reset when the command returns so a test process does not keep labelling
    # later events as cli. `serve` holds the process, so its requests still inherit this.
    token = event_actor.set("cli")
    ctx.call_on_close(lambda: event_actor.reset(token))


_ENV = typer.Option(
    "prod",
    "--env",
    "-e",
    envvar="INTERLACE_ENV",
    help="Target data environment (prod = the unprefixed namespace).",
)
_PATH = typer.Option(Path("."), "--path", "-p", help="Project root.")
_SELECT = typer.Option([], "--select", "-s", help="Model selectors: name, +name, name+, tag:x.")
_START = typer.Option("", "--start", help="Window start (ISO), for incremental models.")
_END = typer.Option("", "--end", help="Window end (ISO), for incremental models.")
_FORWARD_ONLY = typer.Option(
    False,
    "--forward-only",
    help="Modified history-keeping models (merge/full_merge/hash_merge/scd/incremental) carry their "
    "history forward: it is copied to the new version, the new logic applies to the copy, and "
    "checks gate before views move. Requires a shape-compatible change.",
)
_JSON = typer.Option(False, "--json", help="Emit JSON instead of a table (for scripts and CI).")
_PARALLELISM = typer.Option(
    0,
    "--parallelism",
    min=0,
    help="How many models build at once (0 = the project's `parallelism`, default 4). Use 1 to serialise.",
)


def _emit_json(data: object) -> None:
    import json

    typer.echo(json.dumps(data, indent=2, default=str))


def _table(title: str) -> Table:
    """The house table style: no grid, a thin rule under the header, left title.

    Primary columns keep the default style; add secondary columns with
    ``style="dim"`` so the eye lands on the values that matter.
    """
    return Table(
        title=title,
        title_justify="left",
        title_style="bold",
        box=box.SIMPLE_HEAD,
        border_style="dim",
        pad_edge=False,
    )


def _render_build_results(result: ApplyResult, compiled: CompiledProject) -> None:
    """Per-model outcome table: what was built, how it writes, where it ran, what
    it read, what it did to the rows, and how long."""
    built = set(result.built)
    if not built:
        return
    table = _table("Build results")
    table.add_column("Model")
    table.add_column("Output", style="dim")
    table.add_column("Strategy", style="dim")
    table.add_column("Engine", style="dim")
    table.add_column("Depends on", style="dim", no_wrap=True)
    table.add_column("Rows", justify="right")
    table.add_column("Time", justify="right", style="dim")
    for model in compiled.ordered():
        if model.name not in built:
            continue
        counts = result.rows.get(model.name)
        parts = []
        if counts is not None:
            if counts.inserted:
                parts.append(f"[green]+{counts.inserted:,}[/]")
            if counts.updated:
                parts.append(f"[yellow]~{counts.updated:,}[/]")
            if counts.deleted:
                parts.append(f"[red]-{counts.deleted:,}[/]")
        seconds = result.timings.get(model.name)
        table.add_row(
            model.name,
            model.materialise,
            model.strategy,
            model.engine,
            ", ".join(model.dependencies) or "—",
            " ".join(parts) or "[dim]—[/]",
            f"{seconds:.2f}s" if seconds is not None else "—",
        )
    console.print(table)


def _render_checks(result: ApplyResult) -> None:
    if not result.checks:
        return
    passed = sum(1 for c in result.checks if c.status == "passed")
    warned = [c for c in result.checks if c.status != "passed"]
    line = f"Checks: {passed}/{len(result.checks)} passed"
    if warned:
        line += "; " + ", ".join(f"[yellow]{c.model}.{c.name} {c.status} ({c.severity})[/yellow]" for c in warned)
    console.print(line)


def _render_warnings(plan_result: Plan) -> None:
    for warning in plan_result.warnings:
        console.print(f"[yellow]note:[/yellow] {warning}")


async def _render_empty_incrementals(result: ApplyResult, compiled: CompiledProject, engines: Any) -> None:
    """A built incremental model that wrote nothing AND whose table is empty holds
    no data — reporting success without saying so is how people conclude "no data".
    (The default window is the most recent grain; historical data needs --start/--end.)"""
    import sqlglot

    for name in result.built:
        model = compiled.models[name]
        if model.strategy != "incremental" or model.is_terminal:
            continue
        counts = result.rows.get(name)
        if counts is not None and (counts.inserted or counts.updated):
            continue
        with contextlib.suppress(Exception):
            engine = engines.require(model.engine)
            table = model.physical_table.to_expr().sql(dialect=engine.dialect)
            reader = await engine.fetch(sqlglot.parse_one(f"SELECT count(*) FROM {table}", read=engine.dialect))
            if int(reader.read_all().column(0)[0].as_py()) == 0:
                console.print(
                    f"[yellow]note:[/yellow] {name} is empty — its windows covered no source data. "
                    f"Backfill the real range: [bold]interlace run --select {name} --start <ISO> --end <ISO>[/bold]"
                )


def _selection(
    compiled: CompiledProject, selectors: list[str], promoted: dict[str, str] | None = None
) -> set[str] | None:
    if not selectors:
        return None
    try:
        return select_models(selectors, compiled, promoted=promoted)
    except SelectionError as exc:
        console.print(f"[red]{escape(exc.message)}[/red]")
        raise typer.Exit(1) from exc


def _window(value: str, flag: str) -> datetime | None:
    if not value:
        return None
    from interlace.state.interval import naive_local

    try:
        return naive_local(value)
    except ValueError as exc:
        console.print(f"[red]{flag} must be an ISO timestamp (e.g. 2026-07-01T00:00:00); got {value!r}[/red]")
        raise typer.Exit(2) from exc


def _render(plan: Plan, environment: str) -> None:  # noqa: C901
    if not plan.changes and not plan.physical and not plan.transfers and not plan.drift and not plan.warnings:
        console.print(f"No changes for [bold]{environment}[/bold].")
        return
    reused = {snapshot.name for snapshot in plan.reuses}
    if plan.changes:
        table = _table(f"Plan · {environment}")
        table.add_column("Model")
        table.add_column("Change")
        table.add_column("Category")
        table.add_column("Build")
        change_colours = {"added": "green", "removed": "red", "modified": "yellow"}
        category_colours = {"breaking": "red", "non_breaking": "green", "forward_only": "cyan"}
        for change in plan.changes:
            build = (
                "[cyan]reuse[/]"
                if change.name in reused
                else ("[dim]—[/]" if change.change_type is ChangeType.REMOVED else "rebuild")
            )
            kind = change.change_type.value
            category = change.category.value if change.category else None
            table.add_row(
                change.name,
                f"[{change_colours.get(kind, 'white')}]{kind}[/]",
                f"[{category_colours.get(category, 'white')}]{category}[/]" if category else "[dim]—[/]",
                build,
            )
        console.print(table)
    if reused:
        console.print(f"[dim]{len(reused)} model(s) have provably identical output — reusing existing tables.[/dim]")
    for action in plan.physical:
        for physical in action.changes:
            mark = "[green]+[/]" if physical.op == "add" else "[red]-[/]"
            console.print(f"{mark} {physical.kind} {physical.name}  [dim]{action.name}[/]")
        for warning in action.warnings:
            console.print(f"[yellow]note:[/yellow] {warning}")
    for transfer in plan.transfers:
        console.print(
            f"[cyan]transfer[/cyan] {transfer.model}: {transfer.source.name} → {transfer.target.name} "
            f"({transfer.via} → {transfer.table.schema}.{transfer.table.name})"
        )
    for note in plan.drift:
        style = "red" if note.blocking else "yellow"
        console.print(f"[{style}]drift:[/{style}] {note.message}")
    for warning in plan.warnings:
        if any(warning == note.message for note in plan.drift):
            continue
        if any(warning in action.warnings for action in plan.physical):
            continue
        console.print(f"[yellow]note:[/yellow] {warning}")


def _flatten_exceptions(exc: BaseException) -> list[BaseException]:
    """Leaf exceptions of a (possibly nested) ExceptionGroup — a parallel apply
    reports its failures as one."""
    if isinstance(exc, BaseExceptionGroup):
        return [leaf for sub in exc.exceptions for leaf in _flatten_exceptions(sub)]
    return [exc]


def _print_error(exc: InterlaceError) -> None:
    err_console.print(f"[red]error:[/red] {escape(exc.message)}")
    statement = exc.details.get("statement")
    if isinstance(statement, str) and statement:
        err_console.print(escape(statement))
