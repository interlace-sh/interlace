"""Read-only commands: models, runs, checks, streams, lineage, query."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import typer
from rich.markup import escape

from interlace.cli.support import (
    _ENV,
    _JSON,
    _PATH,
    _SELECT,
    _emit_json,
    _selection,
    _table,
    app,
    console,
)
from interlace.exceptions import (
    PlanError,
    QueryError,
    SelectionError,
)
from interlace.graph.column_lineage import column_impact, column_lineage, split_target
from interlace.graph.project import CompiledProject
from interlace.project import Project
from interlace.streaming import ensure_stream_tables


@app.command()
def query(
    sql: str = typer.Argument(..., help="A read-only SELECT to run against the warehouse."),
    path: Path = _PATH,
    limit: int = typer.Option(100, "--limit", "-n", help="Maximum rows to display (max 10,000)."),
) -> None:
    """Run a read-only SELECT against the warehouse and print the result.

    SELECT only — real tables and views, not table functions or files (the same fence
    as the web console). Unqualified names resolve to the promoted (prod) views:

        interlace query "SELECT * FROM raw_events"
    """
    asyncio.run(_query(sql, path, limit))


async def _query(sql: str, path: Path, limit: int) -> None:
    from interlace.query import prepare_readonly

    project = Project.load(path)
    engines = project.open_engines()
    try:
        engine = engines.get()
        bounded, cap = prepare_readonly(sql, engine.dialect, limit)
        try:
            reader = await engine.fetch(bounded)
            columns = list(reader.schema.names)
            records = await asyncio.to_thread(lambda: reader.read_all().to_pylist())
        except QueryError:
            raise
        except Exception as exc:  # engine errors (missing table, bad column) are the user's feedback
            raise QueryError(str(exc)) from exc
    finally:
        engines.close()
    _render_query(columns, records, cap)


def _render_query(columns: list[str], records: list[dict], cap: int) -> None:
    truncated = len(records) > cap
    shown = records[:cap]
    table = _table("")
    for name in columns:
        table.add_column(name)
    for record in shown:
        table.add_row(*("[dim]NULL[/dim]" if record[name] is None else escape(str(record[name])) for name in columns))
    console.print(table)
    note = f"{len(shown)} row(s)" + (" — truncated; raise --limit for more" if truncated else "")
    console.print(f"[dim]{note}[/dim]")


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

    from interlace.checks.runner import run_promoted_checks

    project = Project.load(path)
    compiled = project.compile()
    engines_registry = project.open_engines()
    state = await project.open_state()
    try:
        try:
            outcomes, skipped = await run_promoted_checks(compiled, state, engines_registry, environment, select)
        except PlanError as exc:
            console.print(f"[red]{escape(exc.message)}[/red]")
            raise typer.Exit(1) from exc
        except SelectionError as exc:
            console.print(f"[red]{escape(exc.message)}[/red]")
            raise typer.Exit(1) from exc
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
def connections(path: Path = _PATH, as_json: bool = _JSON) -> None:
    """Named HTTP and Postgres connections (secrets redacted)."""
    from interlace.connections import redacted

    project = Project.load(path)
    rows = [redacted(name, item) for name, item in sorted(project.config.connections.items())]
    if as_json:
        _emit_json(rows)
        return
    table = _table("Connections")
    table.add_column("Connection")
    table.add_column("Type", style="dim")
    table.add_column("Target", style="dim")
    for row in rows:
        target = str(row.get("dsn") or row.get("base_url") or "—")
        table.add_row(str(row["name"]), str(row["type"]), target)
    console.print(table)


async def _known_columns(project: Project, compiled: CompiledProject, environment: str) -> dict[str, list[str]]:
    """Output column names from each promoted snapshot's physical table."""
    from interlace.inspect import described_outputs

    try:
        engines = project.open_engines()
    except Exception:
        return {}
    state = await project.open_state()
    try:
        described = await described_outputs(compiled, state, engines, environment)
    finally:
        await state.close()
        engines.close()
    return {name: list(columns) for name, columns in described.items()}


@app.command()
def impact(
    target: str = typer.Argument(..., help="model.column — what would changing this column touch?"),
    path: Path = _PATH,
    environment: str = _ENV,
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
    described = asyncio.run(_known_columns(project, compiled, environment))
    result = column_impact(compiled, model, column, known_columns=described)
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
    environment: str = _ENV,
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
    described = asyncio.run(_known_columns(project, compiled, environment)) if columns else {}
    sources = column_lineage(compiled, known_columns=described).get(model, {}) if columns else {}

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
