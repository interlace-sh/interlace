"""
CLI command for promoting data between environments.

Copies tables from one environment/connection to another, enabling
workflows like dev → staging → production data promotion.
"""

from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from interlace.utils.logging import get_logger

logger = get_logger("interlace.cli.promote")
console = Console()

app = typer.Typer(
    name="promote",
    help="Promote data between environments or connections",
    no_args_is_help=True,
)


@app.callback(invoke_without_command=True)
def promote(
    ctx: typer.Context,
    source_env: str = typer.Option(
        ..., "--from", "-f", help="Source environment name (e.g. dev, staging)"
    ),
    target_env: str = typer.Option(
        ..., "--to", "-t", help="Target environment name (e.g. staging, prod)"
    ),
    models: Optional[List[str]] = typer.Argument(
        None, help="Specific models to promote (default: all source-tagged models)"
    ),
    sources_only: bool = typer.Option(
        True,
        "--sources-only/--all",
        help="Only promote source models (tagged 'source'), or all models",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", "-n", help="Show what would be promoted without executing"
    ),
    project_dir: Path = typer.Option(
        Path("."), "--project", "-p", help="Project directory"
    ),
    connection: Optional[str] = typer.Option(
        None, "--connection", "-c",
        help="Specific connection name to use (overrides default connection resolution)",
    ),
):
    """
    Promote data from one environment to another.

    Copies tables between connections/environments. By default, only promotes
    models tagged as 'source' to avoid copying derived/transformed data.

    Examples:
        interlace promote --from dev --to staging
        interlace promote --from staging --to prod --dry-run
        interlace promote --from dev --to staging customer_source
        interlace promote --from dev --to prod --all
    """
    if ctx.invoked_subcommand is not None:
        return

    from interlace.config.loader import load_config

    # Load configs for both environments
    try:
        source_config = load_config(project_dir, env=source_env)
    except FileNotFoundError:
        console.print(f"[red]Source environment config not found: config.{source_env}.yaml[/red]")
        console.print("[dim]Hint: Create a config.{source_env}.yaml file or use 'default' as --from[/dim]")
        raise typer.Exit(1)

    try:
        target_config = load_config(project_dir, env=target_env)
    except FileNotFoundError:
        console.print(f"[red]Target environment config not found: config.{target_env}.yaml[/red]")
        raise typer.Exit(1)

    # Discover models
    from interlace.utils.discovery import discover_models

    models_dir = project_dir / source_config.get("models_dir", "models")
    if not models_dir.exists():
        console.print(f"[red]Models directory not found: {models_dir}[/red]")
        raise typer.Exit(1)

    all_models = discover_models(models_dir)
    if not all_models:
        console.print("[yellow]No models found[/yellow]")
        raise typer.Exit(0)

    # Filter models to promote
    if models:
        # Explicit model list
        promote_models = {k: v for k, v in all_models.items() if k in models}
        missing = set(models) - set(promote_models.keys())
        if missing:
            console.print(f"[yellow]Warning: Models not found: {', '.join(missing)}[/yellow]")
    elif sources_only:
        # Only source-tagged models
        promote_models = {
            k: v for k, v in all_models.items()
            if "source" in (v.get("tags") or [])
        }
        if not promote_models:
            console.print("[yellow]No source-tagged models found.[/yellow]")
            console.print("[dim]Hint: Tag models with tags=['source'] or use --all to promote all models[/dim]")
            raise typer.Exit(0)
    else:
        promote_models = all_models

    # Display plan
    console.print()
    console.print(
        Panel(
            f"[bold]Promote: {source_env} → {target_env}[/bold]\n"
            f"Models: {len(promote_models)}",
            title="Data Promotion",
        )
    )

    plan_table = Table(title="Promotion Plan")
    plan_table.add_column("Model", style="cyan")
    plan_table.add_column("Schema", style="dim")
    plan_table.add_column("Materialisation", style="green")
    plan_table.add_column("Tags", style="yellow")

    for name, info in sorted(promote_models.items()):
        plan_table.add_row(
            name,
            info.get("schema", "public"),
            info.get("materialise", "table"),
            ", ".join(info.get("tags") or ["-"]),
        )

    console.print(plan_table)

    if dry_run:
        console.print("\n[yellow]Dry run - no data was copied[/yellow]")
        raise typer.Exit(0)

    # Confirm
    console.print()
    confirm = typer.confirm(
        f"Promote {len(promote_models)} model(s) from {source_env} to {target_env}?"
    )
    if not confirm:
        console.print("[dim]Cancelled[/dim]")
        raise typer.Exit(0)

    # Execute promotion
    _execute_promotion(
        promote_models=promote_models,
        source_config=source_config,
        target_config=target_config,
        source_env=source_env,
        target_env=target_env,
        connection_name=connection,
    )


def _execute_promotion(
    promote_models: dict,
    source_config,
    target_config,
    source_env: str,
    target_env: str,
    connection_name: Optional[str] = None,
) -> None:
    """Execute the data promotion between environments."""
    from interlace.connections.manager import ConnectionManager as ConnManager

    # Create connection managers for both environments
    try:
        source_mgr = ConnManager(source_config.data, validate=False)
        target_mgr = ConnManager(target_config.data, validate=False)
    except Exception as e:
        console.print(f"[red]Failed to create connections: {e}[/red]")
        raise typer.Exit(1)

    # Determine which connections to use
    source_conn_configs = source_config.data.get("connections", {})
    target_conn_configs = target_config.data.get("connections", {})

    successes = 0
    failures = 0
    skipped = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        task = progress.add_task("Promoting...", total=len(promote_models))

        for model_name, model_info in promote_models.items():
            progress.update(task, description=f"Promoting {model_name}...")

            model_conn_name = connection_name or model_info.get("connection")
            model_schema = model_info.get("schema", "public")
            materialise = model_info.get("materialise", "table")

            # Skip ephemeral models
            if materialise == "ephemeral":
                logger.debug(f"Skipping ephemeral model: {model_name}")
                skipped += 1
                progress.advance(task)
                continue

            try:
                # Resolve source connection
                src_conn_name = model_conn_name or _get_default_conn_name(source_conn_configs)
                tgt_conn_name = model_conn_name or _get_default_conn_name(target_conn_configs)

                try:
                    src_conn = source_mgr.get(src_conn_name)
                except ValueError:
                    src_conn = None
                try:
                    tgt_conn = target_mgr.get(tgt_conn_name)
                except ValueError:
                    tgt_conn = None

                if src_conn is None:
                    console.print(f"  [red]✗[/red] {model_name}: source connection '{src_conn_name}' not found")
                    failures += 1
                    progress.advance(task)
                    continue

                if tgt_conn is None:
                    console.print(f"  [red]✗[/red] {model_name}: target connection '{tgt_conn_name}' not found")
                    failures += 1
                    progress.advance(task)
                    continue

                # Get ibis backend connections
                src_backend = src_conn.connection if hasattr(src_conn, "connection") else src_conn
                tgt_backend = tgt_conn.connection if hasattr(tgt_conn, "connection") else tgt_conn

                # Read data from source
                try:
                    src_table = src_backend.table(model_name, database=model_schema)
                except Exception:
                    # Try without database parameter
                    try:
                        src_table = src_backend.table(f"{model_schema}.{model_name}")
                    except Exception:
                        console.print(f"  [yellow]⊘[/yellow] {model_name}: table not found in source")
                        skipped += 1
                        progress.advance(task)
                        continue

                # Execute to get data (materialise from source)
                df = src_table.execute()
                row_count = len(df)

                # Ensure target schema exists
                try:
                    if hasattr(tgt_backend, "create_database"):
                        tgt_backend.create_database(model_schema, force=False)
                except Exception:
                    pass

                # Write to target (overwrite)
                try:
                    tgt_backend.create_table(model_name, obj=df, database=model_schema, overwrite=True)
                except (TypeError, AttributeError):
                    qualified_name = f"{model_schema}.{model_name}"
                    tgt_backend.create_table(qualified_name, obj=df, overwrite=True)

                logger.info(f"Promoted {model_name}: {row_count} rows from {source_env} to {target_env}")
                successes += 1

            except Exception as e:
                console.print(f"  [red]✗[/red] {model_name}: {e}")
                logger.error(f"Failed to promote {model_name}: {e}", exc_info=True)
                failures += 1

            progress.advance(task)

    # Summary
    console.print()
    summary_parts = []
    if successes:
        summary_parts.append(f"[green]{successes} promoted[/green]")
    if skipped:
        summary_parts.append(f"[yellow]{skipped} skipped[/yellow]")
    if failures:
        summary_parts.append(f"[red]{failures} failed[/red]")

    console.print(Panel(
        ", ".join(summary_parts),
        title=f"Promotion Complete: {source_env} → {target_env}",
    ))

    # Close connections
    for conn_name in source_mgr.list():
        try:
            conn = source_mgr.get(conn_name)
            if hasattr(conn, "close"):
                conn.close()
        except Exception:
            pass
    for conn_name in target_mgr.list():
        try:
            conn = target_mgr.get(conn_name)
            if hasattr(conn, "close"):
                conn.close()
        except Exception:
            pass


def _get_default_conn_name(conn_configs: dict) -> str:
    """Get the default connection name from config."""
    if "default" in conn_configs:
        return "default"
    if conn_configs:
        return next(iter(conn_configs))
    return "default"
