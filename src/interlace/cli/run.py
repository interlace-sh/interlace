"""
interlace run - Execute models.

Phase 0: Execute models and pipelines.
"""

import asyncio
from pathlib import Path

import typer

from interlace.core.executor import execute_models
from interlace.utils.logging import get_logger

logger = get_logger("interlace.cli.run")


app = typer.Typer(name="run", help="Execute Interlace models", invoke_without_command=True)


@app.callback()
def run(
    ctx: typer.Context,
    tasks: list[str] | None = typer.Argument(None, help="Specific models to run (default: all)"),
    env: str | None = typer.Option(None, help="Environment (dev, staging, prod)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    project_dir: Path = typer.Option(Path.cwd(), "--project-dir", "-d", help="Project directory"),
    force: bool = typer.Option(False, "--force", "-f", help="Force execution (bypass change detection)"),
    since: str | None = typer.Option(None, "--since", help="Backfill: override cursor start value (e.g. '2024-01-01')"),
    until: str | None = typer.Option(None, "--until", help="Backfill: upper bound for cursor filter"),
) -> None:
    """
    Execute models.

    Phase 0: Basic execution of models with dependency discovery.
    """
    if ctx.invoked_subcommand is None:
        logger.info("Initializing Interlace...")

        # Initialize Interlace (config, logging, connections, state store, models)
        try:
            from interlace.core.initialization import InitializationError, initialize

            config_obj, state_store, all_models, graph = initialize(project_dir, env=env, verbose=verbose)
            config = config_obj.data
        except InitializationError as e:
            logger.error(f"Initialization failed: {e}")
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1) from e

        if not all_models:
            logger.warning("No models found")
            typer.echo("No models found")
            raise typer.Exit(1)

        # Filter models if specific tasks requested
        if tasks:
            # Only run requested models and their dependencies
            models_to_run = set(tasks)
            # Add dependencies
            for task in tasks:
                deps = graph.get_dependencies(task)
                models_to_run.update(deps)

            all_models = {k: v for k, v in all_models.items() if k in models_to_run}

        # Backfill implies forced re-execution
        if since is not None:
            force = True

        # Note: Logging of "Discovered..." and "Running..." is done inside execute_dynamic()
        # after the Rich header panel is displayed, so we don't log here
        asyncio.run(execute_models(all_models, graph, config, force=force, since=since, until=until))

        # Results are already displayed in the Rich progress panel
        # No need to duplicate here
