"""The ``interlace`` CLI: plan and apply against a project."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime
from pathlib import Path
from typing import Any

import typer
from rich import box
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TaskID, TextColumn, TimeElapsedColumn
from rich.table import Table

from interlace.exceptions import CheckError, ConfigurationError, InterlaceError, SelectionError
from interlace.graph.column_lineage import column_impact, column_lineage, split_target
from interlace.graph.project import CompiledProject
from interlace.graph.selectors import select_models, wants_state
from interlace.plan.apply import ApplyResult
from interlace.plan.apply import apply as apply_plan
from interlace.plan.differ import diff
from interlace.plan.plan import ChangeType, Plan
from interlace.plan.run import run_plan
from interlace.project import Project
from interlace.scaffold import scaffold_project
from interlace.scheduler.engine import TriggerEngine, build_triggers
from interlace.scheduler.worker import drain
from interlace.sinks import target_ref
from interlace.streaming import ensure_stream_tables, flush_streams

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

    def __call__(self, model: str, event: str) -> None:
        if event == "start":
            self._rows[model] = self.progress.add_task(model, total=1, status="")
        elif event == "done":
            self.progress.update(self._rows[model], completed=1, status="[green]✓[/green]")
        elif event == "cancelled":  # collateral of a sibling's failure, not a failure itself
            self.progress.update(self._rows[model], completed=1, status="[dim]⊘[/dim]")
        else:  # failed
            self.progress.update(self._rows[model], completed=1, status="[red]✗[/red]")


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
    version: bool = typer.Option(
        False, "--version", "-v", callback=_version_callback, is_eager=True, help="Show the version and exit."
    ),
) -> None:
    pass


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
    help="Modified history-keeping models (merge/full_merge/scd/incremental_by_time) carry their history "
    "forward: it is copied to the new version, the new logic applies to the copy, and checks gate "
    "before views move. Requires a shape-compatible change.",
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
        if model.strategy != "incremental_by_time" or model.is_terminal:
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
        console.print(f"[red]{exc.message}[/red]")
        raise typer.Exit(1) from exc


async def _promoted_if_needed(state: Any, environment: str, selectors: list[str]) -> dict[str, str] | None:
    """state:modified compares against the target environment's fingerprints."""
    return await state.get_environment(environment) if wants_state(selectors) else None


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
    console.print("\nNext: [bold]interlace apply[/bold] (or --env dev for a sandbox)")


@app.command()
def plan(
    environment: str = _ENV,
    path: Path = _PATH,
    select: list[str] = _SELECT,
    forward_only: bool = _FORWARD_ONLY,
    as_json: bool = _JSON,
) -> None:
    """Show what apply would change in an environment."""
    asyncio.run(_plan(environment, path, select, forward_only, as_json))


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


async def _plan(
    environment: str, path: Path, select: list[str], forward_only: bool = False, as_json: bool = False
) -> None:
    project = Project.load(path)
    compiled = project.compile()
    state = await project.open_state()
    try:
        promoted = await _promoted_if_needed(state, environment, select)
        result = await diff(
            compiled, environment, state, select=_selection(compiled, select, promoted), forward_only=forward_only
        )
        if as_json:
            _emit_json(_plan_dict(result, environment))
        else:
            _render(result, environment)
    finally:
        await state.close()


def _plan_dict(plan: Plan, environment: str) -> dict:
    """The plan as data — mirrors the HTTP API's PlanResponse shape."""
    reused = {snapshot.name for snapshot in plan.reuses}
    return {
        "environment": environment,
        "changes": [
            {
                "name": change.name,
                "change_type": change.change_type.value,
                "category": change.category.value if change.category else None,
                "reused": change.name in reused,
                "previous_fingerprint": change.previous_fingerprint,
                "new_fingerprint": change.new_fingerprint,
            }
            for change in plan.changes
        ],
        "transfers": [
            f"{t.model}: {t.source.name} -> {t.target.name} ({t.via} -> {t.table.schema}.{t.table.name})"
            for t in plan.transfers
        ],
    }


async def _apply(
    environment: str,
    path: Path,
    select: list[str],
    forward_only: bool = False,
    force: bool = False,
    parallelism: int = 0,
) -> None:
    project = Project.load(path)
    compiled = project.compile()
    engines = project.open_engines()
    state = await project.open_state()
    try:
        # Stream-fed projects must build without the daemon ever having run:
        # declared stream tables are ensured (empty) so models reading them work.
        if project.streams:
            await ensure_stream_tables(project.streams, engines.get())
        promoted = await _promoted_if_needed(state, environment, select)
        plan_result = await diff(
            compiled, environment, state, select=_selection(compiled, select, promoted), forward_only=forward_only
        )
        _render(plan_result, environment)
        if plan_result.is_empty:
            return
        if plan_result.has_breaking_changes and not force:  # same guard the HTTP API enforces
            breaking = ", ".join(
                c.name for c in plan_result.changes if c.category is not None and c.category.value == "breaking"
            )
            console.print(f"[red]plan has breaking changes ({breaking}); re-run with --force to proceed[/red]")
            raise typer.Exit(1)
        progress = _build_progress(plan_result)
        try:
            with progress.progress if progress else contextlib.nullcontext():
                result = await apply_plan(
                    plan_result,
                    compiled=compiled,
                    engines=engines,
                    state=state,
                    base_path=project.root,
                    on_progress=progress,
                    parallelism=parallelism or project.config.parallelism,  # --parallelism wins over config
                )
        except CheckError as exc:
            console.print(f"[red]{exc.message}[/red]")
            raise typer.Exit(1) from exc
        _render_build_results(result, compiled)
        _render_checks(result)
        await _render_empty_incrementals(result, compiled, engines)
        console.print(
            f"[green]Built {len(set(result.built))} model(s); promoted {result.promoted} to '{environment}'.[/green]"
        )
    finally:
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

    For incremental_by_time models, --start/--end set the catchup window
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


def _window(value: str, flag: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        console.print(f"[red]{flag} must be an ISO timestamp (e.g. 2026-07-01T00:00:00); got {value!r}[/red]")
        raise typer.Exit(2) from exc
    if parsed.tzinfo is not None:
        # the interval ledger stores naive local timestamps; one aware window would
        # poison every later naive/aware comparison for that model
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed


async def _execute(
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
    try:
        if project.streams:  # as in _apply: stream tables must exist daemon or not
            await ensure_stream_tables(project.streams, engines.get())
        plan_result = await run_plan(
            compiled,
            environment,
            state,
            start=window_start,
            end=window_end,
            select=_selection(compiled, select, await _promoted_if_needed(state, environment, select)),
            restate=restate,
        )
        progress = _build_progress(plan_result)
        try:
            with progress.progress if progress else contextlib.nullcontext():
                result = await apply_plan(
                    plan_result,
                    compiled=compiled,
                    engines=engines,
                    state=state,
                    base_path=project.root,
                    on_progress=progress,
                    parallelism=parallelism or project.config.parallelism,  # --parallelism wins over config
                )
        except CheckError as exc:
            console.print(f"[red]{exc.message}[/red]")
            raise typer.Exit(1) from exc
        _render_build_results(result, compiled)
        _render_checks(result)
        _render_warnings(plan_result)
        await _render_empty_incrementals(result, compiled, engines)
        verb = "Restated" if restate else "Ran"
        console.print(
            f"[green]{verb} {len(set(result.built))} model(s) ({len(result.built)} task(s)); "
            f"promoted {result.promoted} to '{environment}'.[/green]"
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

    try:
        parsed_grace = parse_grain(grace)
    except ValueError as exc:
        console.print(f"[red]--grace must be a grain like 7d or 12h; got {grace!r}[/red]")
        raise typer.Exit(2) from exc
    project = Project.load(path)
    engines = project.open_engines()
    state = await project.open_state()
    try:
        result = await run_gc(state, engines=engines, grace=parsed_grace, dry_run=dry_run)
        verb = "Would remove" if dry_run else "Removed"
        console.print(
            f"{verb} {len(result.removed_snapshots)} snapshot(s), dropped {len(result.dropped_tables)} table(s); "
            f"{result.kept_snapshots} snapshot(s) kept."
        )
        if not dry_run:
            trimmed = await state.trim_logs()
            if any(trimmed.values()):
                console.print(
                    f"Trimmed {trimmed['events']} event(s), {trimmed['check_results']} check result(s), "
                    f"{trimmed['runs']} finished run(s) older than 30 days."
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
    from interlace.streaming.materializer import sweep_streams

    project = Project.load(path)
    compiled = project.compile()
    engines = project.open_engines()
    state = await project.open_state()
    stream_log = await project.open_stream_log() if project.streams else None
    if project.streams:
        await ensure_stream_tables(project.streams, engines.get())
    trigger_engine = TriggerEngine(build_triggers(compiled), state)
    try:
        while True:
            await trigger_engine.tick(datetime.now())
            # materialize anything published since the last tick, then run models
            # (an API-only `serve --no-scheduler` process relies on this loop to flush)
            if stream_log is not None:
                await flush_streams(project.streams, stream_log, engines.get())
            ran = await drain(
                state,
                compiled,
                engines=engines,
                environment=environment,
                base_path=project.root,
                parallelism=project.config.parallelism,
            )
            if ran:
                console.print(f"[green]ran {ran} scheduled run(s) in '{environment}'[/green]")
            if stream_log is not None:
                await sweep_streams(project.streams, stream_log, engines.get())
            if once:
                break
            await asyncio.sleep(interval)
    finally:
        if stream_log is not None:
            await stream_log.close()
        await state.close()
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
) -> None:
    """Run the interlace daemon: HTTP API + scheduler in one process (requires the `service` extra).

    Use --no-scheduler for an API-only process (run `interlace scheduler` separately).
    """
    try:
        import uvicorn

        from interlace.service.app import create_app
    except ImportError as exc:
        console.print("[red]The HTTP API needs the 'service' extra: pip install 'interlaced[service]'[/red]")
        raise typer.Exit(1) from exc
    bound = _free_port(host, port)
    if bound != port:
        console.print(f"[yellow]port {port} is in use — serving on {bound}[/yellow]")
    port = bound
    # Auth is open until the first API key exists (see auth.py). That's fine on
    # loopback; on a routable bind it exposes every endpoint — including the SQL
    # console and apply/gc — to the network unauthenticated. Warn loudly.
    if host not in ("127.0.0.1", "localhost", "::1"):
        keyed = asyncio.run(_has_api_keys(path))
        if not keyed:
            console.print(
                f"[bold red]WARNING[/bold red] serving on [bold]{host}[/bold] with no API keys — "
                "the API is open to the network. Create one first: [bold]interlace apikey create <name> --scope admin[/bold]"
            )
    console.print(f"UI at [bold cyan]http://{host}:{port}/ui[/bold cyan]")
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
        # SSE clients (/events/stream) hold their response open forever; without a
        # bound, uvicorn's graceful shutdown waits on them indefinitely ("Waiting
        # for connections to close") and Ctrl+C appears to hang. Lifespan cleanup
        # (flush, store/engine close) runs only after this drain completes.
        timeout_graceful_shutdown=3,
    )


@app.command("models")
def list_models(path: Path = _PATH, select: list[str] = _SELECT, as_json: bool = _JSON) -> None:
    """List models with their materialisation, strategy, engine, and dependencies."""
    project = Project.load(path)
    compiled = project.compile()
    chosen = _selection(compiled, select)
    rows: list[dict[str, Any]] = [
        {
            "name": name,
            "output": compiled.models[name].materialise,
            "strategy": compiled.models[name].strategy,
            "engine": compiled.models[name].engine,
            "depends_on": list(compiled.models[name].dependencies),
        }
        for name in compiled.graph.topological_sort()
        if chosen is None or name in chosen
    ]
    if as_json:
        _emit_json(rows)
        return
    multi_engine = len({m.engine for m in compiled.models.values()}) > 1
    table = _table("Models")
    table.add_column("Model")
    table.add_column("Output", style="dim")
    table.add_column("Strategy", style="dim")
    if multi_engine:
        table.add_column("Engine", style="dim")
    table.add_column("Depends on", style="dim", no_wrap=True)
    for row in rows:
        cells = [row["name"], row["output"], row["strategy"]]
        if multi_engine:
            cells.append(row["engine"])
        table.add_row(*cells, ", ".join(row["depends_on"]) or "—")
    console.print(table)


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
    from interlace.exceptions import PlanError
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
            result = await rollback_environment(state, engines=engines, environment=name, to_generation=to)
        except PlanError as exc:
            console.print(f"[red]{exc.message}[/red]")
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


@app.command()
def runs(
    path: Path = _PATH,
    limit: int = typer.Option(20, "--limit", "-n", help="Rows to show."),
    as_json: bool = _JSON,
) -> None:
    """Recent runs from the durable queue (newest first)."""
    asyncio.run(_runs(path, limit, as_json))


async def _runs(path: Path, limit: int, as_json: bool = False) -> None:
    project = Project.load(path)
    state = await project.open_state()
    try:
        recorded = await state.list_runs(limit)
        if as_json:
            _emit_json(recorded)
            return
        if not recorded:
            console.print(
                "No runs recorded. The queue holds daemon-triggered work — schedules, stream flushes, "
                "POST /runs — while [bold]interlace apply[/bold]/[bold]run[/bold] execute immediately "
                "without enqueueing. Start one with [bold]interlace serve[/bold] or "
                "[bold]interlace scheduler[/bold]."
            )
            return
        table = _table("Runs")
        table.add_column("Id", style="dim")
        table.add_column("State")
        table.add_column("Trigger", style="dim")
        table.add_column("Models")
        table.add_column("Enqueued", style="dim")
        table.add_column("Error")
        state_colours = {"succeeded": "green", "failed": "red", "running": "cyan", "cancelled": "dim"}
        for run in recorded:
            key = str(run["idempotency_key"] or "")
            trigger = key.split(":", 1)[0] if ":" in key else "manual"
            models = ", ".join(run["flow_selector"][:3]) + (" …" if len(run["flow_selector"]) > 3 else "")
            enqueued = str(run["enqueued_at"] or "")[:19]
            state_cell = f"[{state_colours.get(str(run['state']), 'yellow')}]{run['state']}[/]"
            error = f"[red]{str(run['error'])[:60]}[/]" if run["error"] else "[dim]—[/]"
            table.add_row(str(run["id"]), state_cell, trigger, models, enqueued, error)
        console.print(table)
    finally:
        await state.close()


@app.command()
def cancel(run_id: int = typer.Argument(..., help="Run id (see `interlace runs`)."), path: Path = _PATH) -> None:
    """Cancel a run: queued cancels now; running cancels at the worker's next heartbeat."""
    asyncio.run(_cancel(run_id, path))


async def _cancel(run_id: int, path: Path) -> None:
    project = Project.load(path)
    state = await project.open_state()
    try:
        outcome = await state.request_cancel(run_id)
        if outcome is None:
            console.print(f"[red]run {run_id} is unknown or already finished[/red]")
            raise typer.Exit(1)
        console.print(f"run {run_id}: [bold]{outcome}[/bold]")
    finally:
        await state.close()


checks_app = typer.Typer(no_args_is_help=True, help="Run and inspect data-quality checks.")
app.add_typer(checks_app, name="checks")


@checks_app.command("list")
def checks_list(
    path: Path = _PATH,
    model: str = typer.Option("", "--model", "-m", help="Filter to one model."),
    limit: int = typer.Option(20, "--limit", "-n", help="Rows to show."),
    as_json: bool = _JSON,
) -> None:
    """Recent data-quality check results (newest first)."""
    asyncio.run(_checks(path, model or None, limit, as_json))


@checks_app.command("run")
def checks_run(environment: str = _ENV, path: Path = _PATH, select: list[str] = _SELECT, as_json: bool = _JSON) -> None:
    """Run checks against an environment's promoted tables — no rebuild.

    Results are recorded, so `interlace checks list` shows them. Exits 1 when
    any error-severity check fails.
    """
    asyncio.run(_checks_run(environment, path, select, as_json))


async def _checks_run(environment: str, path: Path, select: list[str], as_json: bool = False) -> None:
    from dataclasses import asdict

    from interlace.checks.runner import CheckOutcome, run_checks

    project = Project.load(path)
    compiled = project.compile()
    chosen = _selection(compiled, select)
    engines_registry = project.open_engines()
    state = await project.open_state()
    try:
        promoted = await state.get_environment(environment)
        if not promoted:
            console.print(f"[red]no environment {environment!r} — run `interlace apply` first[/red]")
            raise typer.Exit(1)
        snapshots = await state.get_snapshots(promoted.items())
        physical = {name: snapshot.physical_table for (name, _), snapshot in snapshots.items()}
        outcomes: list[CheckOutcome] = []
        skipped: list[str] = []
        for name, model in compiled.models.items():
            if chosen is not None and name not in chosen:
                continue
            if not model.checks and not compiled.python_checks.get(name):
                continue
            if model.materialise == "file" or (model.is_terminal and environment not in model.environments):
                continue  # a file has no queryable table; a table not delivered in this env has nothing to re-check
            snapshot = snapshots.get((name, promoted.get(name, "")))
            if snapshot is None:  # declared but never promoted here: nothing to check against
                skipped.append(name)
                continue
            # the SNAPSHOT's engine, not the compiled model's: an engine re-pin since
            # the last promote means the promoted table still lives on the old engine
            engine = engines_registry.require(snapshot.engine, model=name)
            check_table = target_ref(model.target or "") if model.materialise == "table" else snapshot.physical_table
            results = await run_checks(
                model, compiled, engine, check_table, compiled.python_checks.get(name, ()), physical
            )
            if results:
                await state.record_check_results(environment, snapshot.fingerprint, results)
            outcomes.extend(results)
    finally:
        await state.close()
        engines_registry.close()

    blocking = [o for o in outcomes if o.blocking]
    if as_json:
        _emit_json([asdict(o) for o in outcomes])
    else:
        colours = {"passed": "green", "failed": "red", "error": "yellow"}
        for outcome in outcomes:
            colour = colours.get(outcome.status, "white")
            failures = f" ({outcome.failures} failing)" if outcome.failures else ""
            console.print(f"[{colour}]{outcome.status:6}[/] {outcome.model}.{outcome.name}{failures}")
        for name in skipped:
            console.print(f"[dim]skip   {name} — not promoted in '{environment}'[/dim]")
        passed = sum(1 for o in outcomes if o.status == "passed")
        console.print(f"Checks: {passed}/{len(outcomes)} passed against '{environment}'.")
    if blocking:
        raise typer.Exit(1)


async def _checks(path: Path, model: str | None, limit: int, as_json: bool = False) -> None:
    project = Project.load(path)
    state = await project.open_state()
    try:
        rows = await state.list_check_results(model, limit)
        if as_json:
            _emit_json(rows)
            return
        table = _table("Check results")
        table.add_column("Env", style="dim")
        table.add_column("Model")
        table.add_column("Check")
        table.add_column("Severity", style="dim")
        table.add_column("Status")
        table.add_column("Failures", justify="right")
        table.add_column("At", style="dim")
        colours = {"passed": "green", "failed": "red", "error": "yellow"}
        for row in rows:
            status = str(row["status"])
            table.add_row(
                str(row["environment"]),
                str(row["model"]),
                str(row["check_name"]),
                str(row["severity"]),
                f"[{colours.get(status, 'white')}]{status}[/]",
                str(row["failures"] or "—"),
                str(row["executed_at"])[:19],
            )
        console.print(table)
    finally:
        await state.close()


@app.command()
def streams(path: Path = _PATH, as_json: bool = _JSON) -> None:
    """Declared streams with their log head and warehouse watermark."""
    asyncio.run(_streams(path, as_json))


async def _streams(path: Path, as_json: bool = False) -> None:
    from interlace.streaming.materializer import stream_watermark

    project = Project.load(path)
    if not project.streams:
        _emit_json([]) if as_json else console.print("No streams declared.")
        return
    engines = project.open_engines()
    log = await project.open_stream_log()
    try:
        engine = engines.get()
        await ensure_stream_tables(project.streams, engine)
        rows: list[dict[str, Any]] = []
        for stream in project.streams:
            head = await log.head(stream.name)
            watermark = await stream_watermark(stream, engine)
            rows.append(
                {
                    "name": stream.name,
                    "table": f"streams.{stream.name}",
                    "on_schema_drift": stream.on_schema_drift,
                    "retention": stream.retention,
                    "head": head,
                    "watermark": watermark,
                    "pending": max(0, head - watermark),
                }
            )
        if as_json:
            _emit_json(rows)
            return
        table = _table("Streams")
        table.add_column("Stream")
        table.add_column("Table", style="dim")
        table.add_column("Drift", style="dim")
        table.add_column("Retention", style="dim")
        table.add_column("Head", justify="right")
        table.add_column("Watermark", justify="right")
        table.add_column("Pending", justify="right")
        for row in rows:
            table.add_row(
                row["name"],
                row["table"],
                row["on_schema_drift"],
                row["retention"] or "—",
                str(row["head"]),
                str(row["watermark"]),
                f"[yellow]{row['pending']}[/]" if row["pending"] else "[dim]—[/]",
            )
        console.print(table)
    finally:
        await log.close()
        engines.close()


@app.command()
def engines(path: Path = _PATH, as_json: bool = _JSON) -> None:
    """Configured execution engines (models pin to these with `engine:`)."""
    from interlace.config.config import redact_dsn

    project = Project.load(path)
    configs = project.config.engine_configs()
    rows: list[dict[str, Any]] = []
    for name in sorted(configs):
        cfg = configs[name]
        rows.append(
            {
                "name": name,
                "default": name == project.config.default_engine,
                "type": cfg.type,
                "dialect": cfg.resolved_dialect(),
                "database": redact_dsn(cfg.database or ""),
            }
        )
    if as_json:
        _emit_json(rows)
        return
    table = _table("Engines")
    table.add_column("Engine")
    table.add_column("Type", style="dim")
    table.add_column("Dialect", style="dim")
    table.add_column("Database", style="dim")
    for row in rows:
        marker = " (default)" if row["default"] else ""
        table.add_row(f"{row['name']}{marker}", row["type"], row["dialect"], row["database"] or "—")
    console.print(table)


@app.command()
def impact(
    target: str = typer.Argument(..., help="model.column — what would changing this column touch?"),
    path: Path = _PATH,
    as_json: bool = _JSON,
) -> None:
    """Column-level blast radius: every downstream column derived from this one,
    transitively, plus models that consume the source whole (Python / ``*``)."""
    project = Project.load(path)
    compiled = project.compile()
    parsed = split_target(target, compiled)
    if parsed is None:
        console.print(f"[red]expected <model>.<column> with a known model; got {target!r}[/red]")
        raise typer.Exit(1)
    model, column = parsed
    result = column_impact(compiled, model, column)
    impacted = result["impacted"]
    opaque = result["opaque_consumers"]

    if as_json:
        _emit_json(result)
        return
    if not impacted and not opaque:
        console.print(f"Nothing downstream reads [bold]{model}.{column}[/bold].")
        return
    table = _table(f"Impact of {model}.{column}")
    table.add_column("Model")
    table.add_column("Column")
    table.add_column("Via", style="dim")
    for row in impacted:
        table.add_row(row["model"], row["column"], row["via"])
    console.print(table)
    if opaque:
        console.print(
            f"[yellow]opaque consumers (see every column):[/yellow] {', '.join(opaque)} "
            "[dim]— Python models or * projections[/dim]"
        )


@app.command()
def lineage(
    model: str = typer.Argument(..., help="Model name."),
    path: Path = _PATH,
    columns: bool = typer.Option(False, "--columns", "-c", help="Show column-level lineage."),
    fmt: str = typer.Option("text", "--format", "-f", help="Output format: text, json, or dot (Graphviz)."),
) -> None:
    """Show a model's lineage — table-level, or column-level with --columns."""
    project = Project.load(path)
    compiled = project.compile()
    if model not in compiled.models:
        console.print(f"[red]unknown model: {model}[/red]")
        raise typer.Exit(1)
    if fmt not in ("text", "json", "dot"):
        console.print(f"[red]unknown format {fmt!r}; expected text, json, or dot[/red]")
        raise typer.Exit(2)

    upstream = sorted(compiled.graph.ancestors(model))
    downstream = sorted(compiled.graph.descendants(model))
    sources = column_lineage(compiled).get(model, {}) if columns else {}

    if fmt == "dot":
        typer.echo(_lineage_dot(compiled, model, upstream, downstream, sources))
        return
    if fmt == "json":
        data: dict = {"model": model, "upstream": upstream, "downstream": downstream}
        if columns:
            data["columns"] = {out: [f"{table}.{col}" for table, col in refs] for out, refs in sources.items()}
        _emit_json(data)
        return

    if columns:
        console.print(f"[bold]{model}[/bold] columns")
        if not sources:
            console.print("  (column lineage unavailable)")
        for output, refs in sources.items():
            rendered = ", ".join(f"{table}.{column}" for table, column in refs) or "—"
            console.print(f"  {output} ← {rendered}")
        return
    console.print(f"[bold]{model}[/bold]")
    console.print(f"  upstream:   {', '.join(upstream) or '—'}")
    console.print(f"  downstream: {', '.join(downstream) or '—'}")


def _lineage_dot(
    compiled: CompiledProject,
    model: str,
    upstream: list[str],
    downstream: list[str],
    sources: dict[str, list[tuple[str, str]]],
) -> str:
    """The model's dependency neighbourhood as a Graphviz digraph (pipe to `dot -Tsvg`)."""

    def node(name: str) -> str:
        return '"' + name.replace('"', '\\"') + '"'

    subgraph = {model, *upstream, *downstream}
    lines = ["digraph lineage {", "  rankdir=LR;", f"  {node(model)} [style=bold];"]
    for name in sorted(subgraph):
        for dep in compiled.models[name].dependencies:
            if dep in subgraph:
                lines.append(f"  {node(dep)} -> {node(name)};")
    for output, refs in sources.items():  # column edges when --columns
        for table, column in refs:
            lines.append(f"  {node(f'{table}.{column}')} -> {node(f'{model}.{output}')} [color=gray];")
    lines.append("}")
    return "\n".join(lines)


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
        keys = await state.list_api_keys()
        matching = sum(1 for key in keys if key["name"] == name)
        if matching and matching == len(keys):
            console.print(
                "[red]refusing to revoke the last key(s) — that would disable authentication; "
                "create a replacement first[/red]"
            )
            raise typer.Exit(1)
        removed = await state.revoke_api_key(name)
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


def _render(plan: Plan, environment: str) -> None:
    if plan.is_empty:
        console.print(f"No changes for [bold]{environment}[/bold].")
        return
    reused = {snapshot.name for snapshot in plan.reuses}
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
    for transfer in plan.transfers:
        console.print(
            f"[cyan]transfer[/cyan] {transfer.model}: {transfer.source.name} → {transfer.target.name} "
            f"({transfer.via} → {transfer.table.schema}.{transfer.table.name})"
        )


def main() -> None:
    try:
        app()
    except InterlaceError as exc:  # expected, user-facing errors: one clean line, no traceback
        err_console.print(f"[red]error:[/red] {exc.message}")
        raise SystemExit(1) from None
