"""
interlace info - Display project information.

Shows discovered models, connections, and project configuration.
"""

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(name="info", help="Display Interlace project information", invoke_without_command=True)

console = Console()


@app.callback()
def info(
    ctx: typer.Context,
    env: str | None = typer.Option(None, help="Environment"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    project_dir: Path = typer.Option(Path.cwd(), "--project-dir", "-d", help="Project directory"),
) -> None:
    """
    Display information about Interlace project and models.
    """
    if ctx.invoked_subcommand is None:
        from interlace.config.loader import load_config
        from interlace.utils.discovery import discover_models

        # Load configuration
        config = load_config(project_dir, env=env)

        # Get project info
        project_name = config.get("name", project_dir.name)
        description = config.get("description", "")

        console.print(f"\n[bold blue]{project_name}[/bold blue]")
        if description:
            console.print(f"[dim]{description}[/dim]")
        console.print()

        # Show connections
        connections = config.get("connections", {})
        if connections:
            conn_table = Table(title="Connections", show_header=True)
            conn_table.add_column("Name", style="cyan")
            conn_table.add_column("Type", style="green")
            conn_table.add_column("Details", style="dim")

            for name, conn_config in connections.items():
                conn_type = conn_config.get("type", "unknown") if isinstance(conn_config, dict) else str(conn_config)
                details = ""
                if isinstance(conn_config, dict):
                    cfg = conn_config.get("config", {})
                    if conn_type == "duckdb":
                        details = cfg.get("path", ":memory:")
                    elif conn_type == "postgres":
                        details = f"{cfg.get('host', 'localhost')}:{cfg.get('port', 5432)}/{cfg.get('database', '')}"
                conn_table.add_row(name, conn_type, details)

            console.print(conn_table)
            console.print()

        # Discover and show models
        models_path = config.get("paths", {}).get("models", "models")
        models_dir = project_dir / models_path

        if models_dir.exists():
            models = discover_models(models_dir)

            if models:
                model_table = Table(title=f"Models ({len(models)})", show_header=True)
                model_table.add_column("Name", style="cyan")
                model_table.add_column("Type", style="green")
                model_table.add_column("Materialization", style="yellow")
                model_table.add_column("Strategy", style="magenta")
                model_table.add_column("Dependencies", style="dim")

                for name, model_info in sorted(models.items()):
                    model_type = model_info.get("type", "python")
                    materialise = model_info.get("materialise", "table")
                    strategy = model_info.get("strategy", "") or ""
                    deps = model_info.get("dependencies", []) or []
                    deps_str = ", ".join(deps) if deps else "-"

                    model_table.add_row(name, model_type, materialise, strategy, deps_str)

                console.print(model_table)

                if verbose:
                    console.print()
                    for name, model_info in sorted(models.items()):
                        console.print(f"[bold]{name}[/bold]")
                        console.print(f"  File: [dim]{model_info.get('file', 'unknown')}[/dim]")
                        if model_info.get("description"):
                            console.print(f"  Description: {model_info.get('description')}")
                        console.print()
            else:
                console.print("[dim]No models found[/dim]")
        else:
            console.print(f"[yellow]Models directory not found: {models_dir}[/yellow]")

        # Show environment info
        if env:
            console.print(f"\n[dim]Environment: {env}[/dim]")
