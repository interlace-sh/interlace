"""``interlace import`` — bring a project across from another tool."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.markup import escape

from interlace.cli.support import app, console
from interlace.exceptions import ConfigurationError
from interlace.importers.dbt import import_dbt

import_app = typer.Typer(no_args_is_help=True, help="Import a project from another tool.")
app.add_typer(import_app, name="import")


@import_app.command("dbt")
def import_dbt_cmd(
    source: Path = typer.Argument(
        ..., exists=True, file_okay=False, help="dbt project directory (holds dbt_project.yml)."
    ),
    out: Path = typer.Option(
        Path("interlace_import"), "--out", "-o", help="Directory to write the interlace project into."
    ),
    force: bool = typer.Option(False, "--force", help="Write into a directory that already has files."),
) -> None:
    """Convert a dbt project: refs, sources, seeds, tests and snapshots.

    Jinja that is a list loop or a SQL-fragment macro is expanded. Anything else
    is left in ``unconverted/`` and listed in ``IMPORT.md``. The project is not
    applied; run ``interlace plan`` on ``--out`` to see what you got.
    """
    try:
        report = import_dbt(source, out, force=force)
    except ConfigurationError as exc:
        console.print(f"[red]{escape(exc.message)}[/red]")
        raise typer.Exit(1) from exc
    console.print(
        f"[green]Imported[/green] {report.project}: {len(report.models)} models, "
        f"{len(report.seeds)} seeds, {report.checks} checks → {report.output}"
    )
    skipped = report.skipped
    if skipped:
        console.print(f"[yellow]{len(skipped)} not converted[/yellow] (see IMPORT.md):")
        for note in skipped:
            console.print(f"  [yellow]·[/yellow] {escape(note.name)} — {escape(note.detail)}")
    else:
        warnings = [note for note in report.notes if note.kind == "warning"]
        if warnings:
            console.print(f"[dim]{len(warnings)} note(s) in IMPORT.md[/dim]")
        else:
            console.print("[dim]Nothing was left unconverted.[/dim]")
