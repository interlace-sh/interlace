"""Plan, apply, run, diff, test, and init."""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Any

import typer
from rich.markup import escape

from interlace.cli.support import (
    _END,
    _ENV,
    _FORWARD_ONLY,
    _JSON,
    _PARALLELISM,
    _PATH,
    _SELECT,
    _START,
    _BuildProgress,
    _emit_json,
    _render,
    _render_build_results,
    _render_checks,
    _render_empty_incrementals,
    _render_warnings,
    _selection,
    _table,
    _window,
    app,
    console,
)
from interlace.exceptions import (
    CheckError,
    ConfigurationError,
    InterlaceError,
    LockError,
    PlanError,
    SelectionError,
)
from interlace.plan.comment import plan_markdown
from interlace.plan.orchestrate import compute_plan, plan_and_apply, resolve_selection, run_and_apply
from interlace.plan.plan import Plan
from interlace.plan.table_diff import TableDiff
from interlace.project import Project, open_session
from interlace.scaffold import list_templates, scaffold_project
from interlace.streaming import ensure_stream_tables


@app.command()
def init(
    path: Path = typer.Argument(Path("."), help="Directory to initialise."),
    name: str = typer.Option("", "--name", "-n", help="Project name (defaults to the directory name)."),
    template: str = typer.Option("quickstart", "--template", "-t", help="Which starter to scaffold (see --list)."),
    show_list: bool = typer.Option(False, "--list", help="List available templates and exit."),
) -> None:
    """Scaffold a new interlace project from a template."""
    if show_list:
        table = _table("Templates")
        table.add_column("Template")
        table.add_column("Description", style="dim")
        table.add_column("Needs", style="dim")
        for info in list_templates():
            # escape: a description may contain [sources]-style brackets Rich would eat as markup
            table.add_row(info.name, escape(info.description), ", ".join(info.requires_env) or "—")
        console.print(table)
        return
    try:
        written = scaffold_project(path, name or None, template)
    except ConfigurationError as exc:
        console.print(f"[red]{escape(exc.message)}[/red] ({exc.details.get('path', '')})")
        raise typer.Exit(1) from exc
    console.print(f"[green]Initialised interlace project in {path}[/green] [dim](template: {template})[/dim]")
    for written_path in written:
        console.print(f"  + {written_path}")
    needs = next((t.requires_env for t in list_templates() if t.name == template), ())
    if needs:
        console.print(f"\n[yellow]Set before applying:[/yellow] {', '.join(needs)}")
    console.print("\nNext: [bold]interlace apply[/bold] (or --env dev for a sandbox)")


@app.command()
def plan(
    environment: str = _ENV,
    path: Path = _PATH,
    select: list[str] = _SELECT,
    forward_only: bool = _FORWARD_ONLY,
    as_json: bool = _JSON,
    markdown: bool = typer.Option(False, "--markdown", help="Emit a GitHub-flavoured plan comment (CI)."),
) -> None:
    """Show what apply would change in an environment."""
    asyncio.run(_plan(environment, path, select, forward_only, as_json, markdown))


@app.command()
def apply(
    environment: str = _ENV,
    path: Path = _PATH,
    select: list[str] = _SELECT,
    forward_only: bool = _FORWARD_ONLY,
    force: bool = typer.Option(False, "--force", help="Proceed even when the plan contains breaking changes."),
    parallelism: int = _PARALLELISM,
) -> None:
    """Build changed models and promote the environment."""
    asyncio.run(_apply(environment, path, select, forward_only, force, parallelism))


@app.command("diff")
def table_diff_cmd(
    environment: str = _ENV,
    path: Path = _PATH,
    select: list[str] = _SELECT,
    against: str = typer.Option("", "--against", help="Other environment (env mode)."),
    source: str = typer.Option("", "--source", help="Left table, schema.table (table mode)."),
    target: str = typer.Option("", "--target", help="Right table, schema.table (table mode)."),
    on: list[str] = typer.Option([], "--on", help="Join key columns. Default: the model's key, else common columns."),
    limit: int = typer.Option(20, "--limit", "-n", help="Sample rows per category."),
    as_json: bool = _JSON,
) -> None:
    """Compare a model across two environments, or two tables on the warehouse."""
    asyncio.run(_table_diff(environment, path, select, against, source, target, on, limit, as_json))


@app.command()
def test(
    path: Path = _PATH,
    select: list[str] = _SELECT,
    update_golden: bool = typer.Option(False, "--update-golden", help="Rewrite tests/golden from the actual result."),
) -> None:
    """Build selected models in an ephemeral DuckDB and diff tests/golden."""
    project = Project.load(path)
    compiled = project.compile()
    from interlace.testing.golden import run_fixture_tests

    chosen = _selection(compiled, select) if select else None
    try:
        report = run_fixture_tests(compiled, project.root, select=chosen, update=update_golden)
    except InterlaceError as exc:
        console.print(f"[red]{escape(exc.message)}[/red]")
        raise typer.Exit(1) from exc
    if update_golden:
        console.print(f"[green]updated {len(report.passed)} golden file(s)[/green]")
        return
    for message in report.messages:
        console.print(f"[red]{escape(message)}[/red]")
    if report.messages:
        raise typer.Exit(1)
    console.print(f"[green]{len(report.passed)} golden test(s) passed[/green]")


async def _plan(
    environment: str,
    path: Path,
    select: list[str],
    forward_only: bool = False,
    as_json: bool = False,
    markdown: bool = False,
) -> None:
    async with open_session(path) as (_project, compiled, state, engines):
        try:
            selected = await resolve_selection(compiled, state, environment, select)
            result = await compute_plan(
                compiled, environment, state, engines, select=selected, forward_only=forward_only
            )
            if markdown:
                typer.echo(plan_markdown(result, environment))
            elif as_json:
                previous = await state.get_snapshots(
                    (change.name, change.previous_fingerprint)
                    for change in result.changes
                    if change.previous_fingerprint is not None
                )
                from interlace.plan.payload import plan_document

                _emit_json(plan_document(result, compiled, previous, environment).as_dict())
            else:
                _render(result, environment)
            if result.blocking:
                raise typer.Exit(1)
        except SelectionError as exc:
            console.print(f"[red]{escape(exc.message)}[/red]")
            raise typer.Exit(1) from exc


def _diff_dict(result: TableDiff) -> dict[str, object]:
    schema = {
        "added": [{"name": name, "type": dtype} for name, dtype in result.schema.added],
        "removed": [{"name": name, "type": dtype} for name, dtype in result.schema.removed],
        "type_changed": [
            {"name": name, "left": left, "right": right} for name, left, right in result.schema.type_changed
        ],
    }
    rows = None
    if result.rows is not None:
        rows = {
            "left_count": result.rows.left_count,
            "right_count": result.rows.right_count,
            "left_only": result.rows.left_only,
            "right_only": result.rows.right_only,
            "changed": result.rows.changed,
            "matched": result.rows.matched,
            "keys": result.rows.keys,
            "left_only_sample": result.rows.left_only_sample,
            "right_only_sample": result.rows.right_only_sample,
            "changed_sample": result.rows.changed_sample,
        }
    return {
        "left": result.left,
        "right": result.right,
        "model": result.model,
        "schema": schema,
        "rows": rows,
        "message": result.message,
        "differs": result.differs,
    }


def _render_diffs(results: list[TableDiff]) -> None:
    for result in results:
        title = result.model or f"{result.left} vs {result.right}"
        if result.message and result.rows is None and result.schema.empty:
            console.print(f"[yellow]{title}[/yellow]  {result.message}")
            continue
        table = _table(title)
        table.add_column("Side")
        table.add_column("Relation", style="dim")
        table.add_row("left", result.left)
        table.add_row("right", result.right)
        console.print(table)
        if result.schema.added or result.schema.removed or result.schema.type_changed:
            for name, dtype in result.schema.added:
                console.print(f"  [green]+[/] column {name} {dtype}")
            for name, dtype in result.schema.removed:
                console.print(f"  [red]-[/] column {name} {dtype}")
            for name, left, right in result.schema.type_changed:
                console.print(f"  [yellow]~[/] column {name} {left} → {right}")
        if result.message:
            console.print(f"  [yellow]{result.message}[/yellow]")
        if result.rows is None:
            continue
        rows = result.rows
        console.print(
            f"  rows  left={rows.left_count} right={rows.right_count}  "
            f"only-left={rows.left_only} only-right={rows.right_only} changed={rows.changed} "
            f"matched={rows.matched}  keys={','.join(rows.keys) or '—'}"
        )


async def _table_diff(
    environment: str,
    path: Path,
    select: list[str],
    against: str,
    source: str,
    target: str,
    on: list[str],
    limit: int,
    as_json: bool,
) -> None:
    from interlace.plan.table_diff import diff_environments, diff_tables, parse_table_ref

    if bool(source) != bool(target):
        console.print("[red]table mode needs both --source and --target[/red]")
        raise typer.Exit(2)
    if not source and not against:
        console.print("[red]env mode needs --against ENV (or pass --source/--target)[/red]")
        raise typer.Exit(2)
    project = Project.load(path)
    compiled = project.compile()
    engines = project.open_engines()
    state = await project.open_state()
    try:
        if source:
            result = await diff_tables(
                engines.get(),
                parse_table_ref(source),
                parse_table_ref(target),
                keys=on or None,
                sample=limit,
            )
            results = [result]
        else:
            selected = await resolve_selection(compiled, state, environment, select)
            results = await diff_environments(
                compiled,
                left_env=environment,
                right_env=against,
                store=state,
                engines=engines,
                select=selected,
                keys=on or None,
                sample=limit,
            )
        if as_json:
            _emit_json([_diff_dict(item) for item in results])
        else:
            if not results:
                console.print("No comparable models (ephemeral/file outputs are skipped).")
            else:
                _render_diffs(results)
        if any(item.differs for item in results):
            raise typer.Exit(1)
    except PlanError as exc:
        console.print(f"[red]{escape(exc.message)}[/red]")
        raise typer.Exit(1) from exc
    finally:
        await state.close()
        engines.close()


async def _apply(  # noqa: C901
    environment: str,
    path: Path,
    select: list[str],
    forward_only: bool = False,
    force: bool = False,
    parallelism: int = 0,
) -> None:
    from interlace.exceptions import BreakingPlanError

    project = Project.load(path)
    compiled = project.compile()
    engines = project.open_engines()
    state = await project.open_state()
    tracker: _BuildProgress | None = _BuildProgress() if console.is_terminal else None
    started = False
    try:

        async def prepare() -> None:
            # Stream-fed projects must build without the daemon ever having run:
            # declared stream tables are ensured (empty) so models reading them work.
            if project.streams:
                await ensure_stream_tables(project.streams, engines.get())

        def on_plan(plan: Plan) -> None:
            nonlocal started, tracker
            _render(plan, environment)
            if tracker is not None and not plan.backfills:
                tracker = None
            if tracker is not None:
                tracker.progress.start()
                started = True

        def on_progress(model: str, event: str, detail: dict[str, Any]) -> None:
            if tracker is not None:
                tracker(model, event, detail)

        try:
            _plan_result, result = await plan_and_apply(
                compiled,
                environment=environment,
                project=project,
                engines=engines,
                state=state,
                lock_owner=f"cli:{os.getpid()}:apply",
                selectors=select,
                forward_only=forward_only,
                force=force,
                parallelism=parallelism or None,
                on_progress=on_progress,
                on_plan=on_plan,
                prepare=prepare,
            )
        except BreakingPlanError as exc:
            console.print(
                f"[red]plan has breaking changes ({', '.join(exc.names)}); re-run with --force to proceed[/red]"
            )
            raise typer.Exit(1) from exc
        except CheckError as exc:
            console.print(f"[red]{escape(exc.message)}[/red]")
            raise typer.Exit(1) from exc
        except LockError as exc:
            console.print(f"[red]{escape(exc.message)}[/red]")
            raise typer.Exit(1) from exc
        if result is None:
            return
        _render_build_results(result, compiled)
        _render_checks(result)
        await _render_empty_incrementals(result, compiled, engines)
        console.print(
            f"[green]Built {len(set(result.built))} model(s); promoted {result.promoted} to '{environment}'.[/green]"
        )
    finally:
        if started and tracker is not None:
            tracker.progress.stop()
        await state.close()
        engines.close()


@app.command()
def run(
    environment: str = _ENV,
    path: Path = _PATH,
    select: list[str] = _SELECT,
    start: str = _START,
    end: str = _END,
    parallelism: int = _PARALLELISM,
) -> None:
    """Force-build models and promote, ignoring change detection.

    For incremental models, --start/--end set the catchup window
    (default: the latest grain interval).
    """
    asyncio.run(_execute(environment, path, select, start, end, restate=False, parallelism=parallelism))


@app.command()
def restate(
    environment: str = _ENV,
    path: Path = _PATH,
    select: list[str] = _SELECT,
    start: str = _START,
    end: str = _END,
    parallelism: int = _PARALLELISM,
) -> None:
    """Reprocess incremental models over a window, ignoring the ledger (vs run, which skips filled)."""
    asyncio.run(_execute(environment, path, select, start, end, restate=True, parallelism=parallelism))


async def _execute(  # noqa: C901
    environment: str,
    path: Path,
    select: list[str],
    start: str,
    end: str,
    *,
    restate: bool,
    parallelism: int = 0,
) -> None:
    window_start = _window(start, "--start")
    window_end = _window(end, "--end")
    project = Project.load(path)
    compiled = project.compile()
    engines = project.open_engines()
    state = await project.open_state()
    tracker: _BuildProgress | None = _BuildProgress() if console.is_terminal else None
    started_bar = False
    try:

        async def prepare() -> None:
            if project.streams:  # as in _apply: stream tables must exist daemon or not
                await ensure_stream_tables(project.streams, engines.get())

        def on_plan(plan: Plan) -> None:
            nonlocal started_bar, tracker
            if tracker is not None and not plan.backfills:
                tracker = None
            if tracker is not None:
                tracker.progress.start()
                started_bar = True

        def on_progress(model: str, event: str, detail: dict[str, Any]) -> None:
            if tracker is not None:
                tracker(model, event, detail)

        started = time.perf_counter()
        try:
            plan_result, result = await run_and_apply(
                compiled,
                environment=environment,
                project=project,
                engines=engines,
                state=state,
                lock_owner=f"cli:{os.getpid()}:run",
                selectors=select,
                start=window_start,
                end=window_end,
                restate=restate,
                parallelism=parallelism or None,
                on_progress=on_progress,
                on_plan=on_plan,
                prepare=prepare,
            )
        except CheckError as exc:
            console.print(f"[red]{escape(exc.message)}[/red]")
            raise typer.Exit(1) from exc
        except LockError as exc:
            console.print(f"[red]{escape(exc.message)}[/red]")
            raise typer.Exit(1) from exc
        _render_build_results(result, compiled)
        _render_checks(result)
        _render_warnings(plan_result)
        await _render_empty_incrementals(result, compiled, engines)
        verb = "Restated" if restate else "Ran"
        console.print(
            f"[green]{verb} {len(set(result.built))} model(s) in {time.perf_counter() - started:.2f}s; "
            f"promoted {result.promoted} to '{environment}'.[/green]"
        )
    finally:
        if started_bar and tracker is not None:
            tracker.progress.stop()
        await state.close()
        engines.close()
