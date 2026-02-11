"""
interlace config - Environment configuration.

Display and manage environment configurations.
"""

import typer
from pathlib import Path
from rich.console import Console
from rich.syntax import Syntax

app = typer.Typer(
    name="config", help="Manage Interlace configurations", invoke_without_command=True
)

console = Console()


@app.callback()
def config(
    ctx: typer.Context,
    env: str = typer.Option(None, help="Specific environment to show"),
    project_dir: Path = typer.Option(Path.cwd(), "--project-dir", "-d", help="Project directory"),
):
    """
    List available environments and configurations.
    """
    if ctx.invoked_subcommand is None:
        # List available config files
        config_files = sorted(project_dir.glob("config*.yaml"))

        if not config_files:
            console.print("[yellow]No configuration files found[/yellow]")
            return

        if env:
            # Show specific environment config
            config_file = project_dir / f"config.{env}.yaml"
            if not config_file.exists():
                config_file = project_dir / f"config.yaml" if env == "default" else None

            if config_file and config_file.exists():
                console.print(f"\n[bold]Configuration: {config_file.name}[/bold]\n")
                content = config_file.read_text()
                syntax = Syntax(content, "yaml", theme="monokai", line_numbers=True)
                console.print(syntax)
            else:
                console.print(f"[red]Configuration not found for environment: {env}[/red]")
                raise typer.Exit(1)
        else:
            # List all environments
            console.print("\n[bold]Available Environments:[/bold]\n")

            for config_file in config_files:
                if config_file.name == "config.yaml":
                    env_name = "default"
                else:
                    env_name = config_file.stem.replace("config.", "")

                # Load and show basic info
                try:
                    from interlace.config.loader import load_config
                    cfg = load_config(project_dir, env=env_name if env_name != "default" else None)
                    project_name = cfg.get("name", "-")
                    connections = len(cfg.get("connections", {}))
                    console.print(f"  [cyan]{env_name}[/cyan] ({config_file.name})")
                    console.print(f"    Name: {project_name}, Connections: {connections}")
                except Exception:
                    console.print(f"  [cyan]{env_name}[/cyan] ({config_file.name})")

            console.print(f"\n[dim]Use 'interlace config --env <name>' to view details[/dim]")
