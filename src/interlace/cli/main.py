"""
Main CLI entry point.
"""

import typer

from interlace import __version__
from interlace.cli import config, info, init, lineage, plan, promote, run, schema, serve, ui
from interlace.migrations import cli as migrate_cli


def version_callback(value: bool):
    """Callback to display version and exit."""
    if value:
        typer.echo(f"interlace version {__version__}")
        raise typer.Exit()


app = typer.Typer(
    name="interlace",
    help="Interlace - A modern data orchestration, transformation and pipeline framework",
    add_completion=True,
)

# Register subcommands
app.add_typer(init.app, name="init")
app.add_typer(run.app, name="run")
app.add_typer(serve.app, name="serve")
app.add_typer(info.app, name="info")
app.add_typer(config.app, name="config")
app.add_typer(migrate_cli.app, name="migrate")
app.add_typer(schema.app, name="schema")
app.add_typer(ui.app, name="ui")
app.add_typer(lineage.app, name="lineage")
app.add_typer(plan.app, name="plan")
app.add_typer(promote.app, name="promote")


@app.callback(invoke_without_command=True)
def entrypoint(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        callback=version_callback,
        help="Show version and exit.",
    ),
):
    """
    Interlace - A modern data orchestration, transformation and pipeline framework.

    Run 'interlace <command> --help' for help on a specific command.
    """
    # Standard behavior: if no subcommand provided, show help (like git, docker, etc.)
    if ctx.invoked_subcommand is None:
        # Version callback already handles exiting, so only show help if version wasn't requested
        if not version:
            typer.echo(ctx.get_help())
            raise typer.Exit()


def main():
    """Main CLI entry point."""
    app()


if __name__ == "__main__":
    main()
