"""
CLI commands for migrations.
"""

import typer
from pathlib import Path
from interlace.migrations.runner import run_migrations, list_pending_migrations
from interlace.migrations.utils import get_all_migrations_with_status
from interlace.core.initialization import initialize
from interlace.utils.logging import get_logger

logger = get_logger("interlace.migrations.cli")

app = typer.Typer(name="migrate", help="Run database migrations", invoke_without_command=True)


@app.callback()
def migrate(
    ctx: typer.Context,
    project_dir: Path = typer.Option(
        Path.cwd(), "--project-dir", "-d", help="Project directory"
    ),
    env: str = typer.Option(
        "dev", "--env", "-e", help="Environment name"
    ),
    migration: str = typer.Option(
        None, "--migration", "-m", help="Specific migration file to run"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be executed without running"
    ),
    list_only: bool = typer.Option(
        False, "--list", "-l", help="List pending migrations without running them"
    ),
):
    """
    Run database migrations.

    Examples:
        # Run all pending migrations
        interlace migrate --env prod

        # Run specific migration
        interlace migrate --migration add_status_column.sql --env prod

        # Dry run (show what would be executed)
        interlace migrate --env prod --dry-run

        # List pending migrations
        interlace migrate --list --env prod
    """
    if ctx.invoked_subcommand is None:
        try:
            # Initialize to get connections
            config, state_store, models, graph = initialize(project_dir, env=env)

            # Get state store connection (migrations use system catalog)
            if not state_store:
                typer.echo("Error: State store not available", err=True)
                raise typer.Exit(1)

            connection = state_store._get_connection()
            if not connection:
                typer.echo("Error: Could not get database connection", err=True)
                raise typer.Exit(1)

            # If --list flag is set, show all migrations with status
            if list_only:
                all_migrations = get_all_migrations_with_status(project_dir, connection, env)
                if not all_migrations:
                    typer.echo("No migrations found")
                    return
                
                # Count by status
                pending_count = sum(1 for m in all_migrations if m["status"] == "pending")
                executed_count = sum(1 for m in all_migrations if m["status"] == "executed")
                failed_count = sum(1 for m in all_migrations if m["status"] == "failed")
                
                typer.echo(f"Migrations ({len(all_migrations)} total):")
                typer.echo(f"  Executed: {executed_count}, Pending: {pending_count}, Failed: {failed_count}\n")
                
                for migration in all_migrations:
                    status = migration["status"]
                    file_name = migration["migration_file"]
                    
                    if status == "executed":
                        executed_at = migration.get("executed_at")
                        executed_by = migration.get("executed_by")
                        timestamp = f" ({executed_at})" if executed_at else ""
                        by = f" by {executed_by}" if executed_by else ""
                        typer.echo(f"  ✓ {file_name}{timestamp}{by}")
                    elif status == "failed":
                        error = migration.get("error_message", "Unknown error")
                        executed_at = migration.get("executed_at")
                        timestamp = f" ({executed_at})" if executed_at else ""
                        typer.echo(f"  ✗ {file_name}{timestamp} - {error}", err=True)
                    else:  # pending
                        typer.echo(f"  ⏳ {file_name} (pending)")
                return

            # Otherwise, run migrations
            results = run_migrations(
                project_dir, connection, env, migration_file=migration, dry_run=dry_run
            )

            if not results:
                typer.echo("No migrations to run")
                return

            # Display results
            success_count = sum(1 for r in results if r.success)
            failure_count = len(results) - success_count

            if dry_run:
                typer.echo(f"\n[DRY RUN] Would execute {len(results)} migration(s):")
                for result in results:
                    typer.echo(f"  ⏳ {result.migration_file}")
                    # Show SQL preview if available
                    if hasattr(result, 'sql_preview') and result.sql_preview:
                        typer.echo(f"     SQL: {result.sql_preview[:200]}...")
            else:
                typer.echo(f"\nExecuted {len(results)} migration(s)")
                typer.echo(f"  Success: {success_count}")
                if failure_count > 0:
                    typer.echo(f"  Failed: {failure_count}", err=True)

                for result in results:
                    if result.success:
                        typer.echo(f"  ✓ {result.migration_file}")
                    else:
                        typer.echo(
                            f"  ✗ {result.migration_file}: {result.error_message}", err=True
                        )

            if failure_count > 0:
                raise typer.Exit(1)

        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)

