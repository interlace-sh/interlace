"""
interlace serve - Long-running service.

Runs Interlace as a long-running HTTP service with:
- POST /runs - Trigger model execution
- POST /sync/run_once - Trigger sync job
- POST /streams/{name} - Publish to stream
- GET /health - Health check
- Background scheduler for models with schedule config
"""

from pathlib import Path

import typer

from interlace.service.server import run_service

app = typer.Typer(name="serve", help="Run Interlace as a long-running service", invoke_without_command=True)


@app.callback()
def serve(
    ctx: typer.Context,
    env: str | None = typer.Option(None, help="Environment (dev, staging, prod)"),
    no_scheduler: bool = typer.Option(False, "--no-scheduler", help="Disable background scheduler"),
    no_ui: bool = typer.Option(False, "--no-ui", help="Disable web UI serving"),
    run: bool = typer.Option(False, "--run", help="Perform initial run of all models on startup"),
    host: str = typer.Option("127.0.0.1", help="Host to bind to"),
    port: int = typer.Option(8080, help="Port to bind to"),
    project_dir: Path = typer.Option(Path.cwd(), "--project-dir", "-d", help="Project directory"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """
    Run Interlace as a long-running service.

    The service provides HTTP endpoints for triggering runs and a background
    scheduler for models configured with schedule metadata.

    By default, serves the web UI at the root URL. Use --no-ui to disable.
    """
    if ctx.invoked_subcommand is None:
        run_service(
            project_dir=project_dir,
            env=env,
            host=host,
            port=port,
            verbose=verbose,
            enable_scheduler=not no_scheduler,
            enable_ui=not no_ui,
            run_on_startup=run,
        )
