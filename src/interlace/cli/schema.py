"""
interlace schema - Schema management commands.

Commands for comparing and managing schemas across environments.
"""

import typer
from pathlib import Path
from typing import Optional
from interlace.core.initialization import initialize, InitializationError
from interlace.utils.logging import get_logger
import ibis

logger = get_logger("interlace.cli.schema")

app = typer.Typer(name="schema", help="Schema management commands")


@app.command("diff")
def schema_diff(
    model_name: str = typer.Argument(..., help="Model name to compare"),
    env1: str = typer.Option("dev", "--env1", "-e1", help="First environment"),
    env2: str = typer.Option("prod", "--env2", "-e2", help="Second environment"),
    project_dir: Path = typer.Option(Path.cwd(), "--project-dir", "-d", help="Project directory"),
    schema: str = typer.Option("public", "--schema", "-s", help="Schema/database name"),
):
    """
    Compare schema of a model between two environments.

    Shows differences in columns, types, and other schema properties.

    Examples:
        # Compare customer model between dev and prod
        interlace schema diff customers --env1 dev --env2 prod

        # Compare with specific schema
        interlace schema diff orders --env1 staging --env2 prod --schema analytics
    """
    try:
        # Initialize both environments
        typer.echo(f"Loading schema from {env1}...")
        try:
            config1, state_store1, models1, graph1 = initialize(project_dir, env=env1)
            conn1 = state_store1._get_connection() if state_store1 else None
        except Exception as e:
            typer.echo(f"Error loading {env1}: {e}", err=True)
            raise typer.Exit(1)

        typer.echo(f"Loading schema from {env2}...")
        try:
            config2, state_store2, models2, graph2 = initialize(project_dir, env=env2)
            conn2 = state_store2._get_connection() if state_store2 else None
        except Exception as e:
            typer.echo(f"Error loading {env2}: {e}", err=True)
            raise typer.Exit(1)

        if not conn1 or not conn2:
            typer.echo("Error: Could not get database connections", err=True)
            raise typer.Exit(1)

        # Get schemas from both environments
        schema1 = None
        schema2 = None

        try:
            table1 = conn1.table(model_name, database=schema)
            schema1 = table1.schema()
        except Exception as e:
            typer.echo(f"Model '{model_name}' not found in {env1} ({schema}): {e}", err=True)
            schema1 = None

        try:
            table2 = conn2.table(model_name, database=schema)
            schema2 = table2.schema()
        except Exception as e:
            typer.echo(f"Model '{model_name}' not found in {env2} ({schema}): {e}", err=True)
            schema2 = None

        # Display comparison
        typer.echo(f"\nSchema comparison for '{model_name}' ({schema}):")
        typer.echo(f"  {env1} vs {env2}\n")

        if schema1 is None and schema2 is None:
            typer.echo(f"  ⚠ Model not found in either environment")
            raise typer.Exit(1)
        elif schema1 is None:
            typer.echo(f"  ⚠ Model exists only in {env2}")
            typer.echo(f"  {env2} columns: {', '.join(schema2.keys())}")
            raise typer.Exit(0)
        elif schema2 is None:
            typer.echo(f"  ⚠ Model exists only in {env1}")
            typer.echo(f"  {env1} columns: {', '.join(schema1.keys())}")
            raise typer.Exit(0)

        # Compare schemas
        cols1 = set(schema1.keys())
        cols2 = set(schema2.keys())

        added = cols2 - cols1
        removed = cols1 - cols2
        common = cols1 & cols2

        # Check for type differences
        type_changes = []
        for col in common:
            type1 = str(schema1[col])
            type2 = str(schema2[col])
            if type1 != type2:
                type_changes.append((col, type1, type2))

        # Display results
        if not added and not removed and not type_changes:
            typer.echo("  ✓ Schemas are identical")
            typer.echo(f"  Columns: {', '.join(sorted(common))}")
        else:
            if added:
                typer.echo(f"  ➕ Added columns in {env2}: {', '.join(sorted(added))}")
            if removed:
                typer.echo(f"  ➖ Removed columns in {env2}: {', '.join(sorted(removed))}")
            if type_changes:
                typer.echo(f"  🔄 Type changes:")
                for col, old_type, new_type in type_changes:
                    typer.echo(f"    - {col}: {old_type} → {new_type}")
            if common:
                typer.echo(f"  ✓ Common columns: {', '.join(sorted(common))}")

    except InitializationError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command("list")
def schema_list(
    env: str = typer.Option("dev", "--env", "-e", help="Environment"),
    project_dir: Path = typer.Option(Path.cwd(), "--project-dir", "-d", help="Project directory"),
    schema: str = typer.Option("public", "--schema", "-s", help="Schema/database name"),
):
    """
    List all models and their schemas in an environment.

    Examples:
        # List all models in dev
        interlace schema list --env dev

        # List models in specific schema
        interlace schema list --env prod --schema analytics
    """
    try:
        config, state_store, models, graph = initialize(project_dir, env=env)

        if not state_store:
            typer.echo("Error: State store not available", err=True)
            raise typer.Exit(1)

        connection = state_store._get_connection()
        if not connection:
            typer.echo("Error: Could not get database connection", err=True)
            raise typer.Exit(1)

        typer.echo(f"\nModels in {env} ({schema}):\n")

        # Get all tables in the schema
        try:
            tables = connection.list_tables(database=schema)
        except (TypeError, AttributeError):
            # Fallback: try without database parameter
            tables = connection.list_tables()

        if not tables:
            typer.echo("  No models found")
            return

        # Display each model's schema
        for table_name in sorted(tables):
            try:
                table = connection.table(table_name, database=schema)
                table_schema = table.schema()
                columns = list(table_schema.keys())
                typer.echo(f"  {table_name}:")
                typer.echo(f"    Columns ({len(columns)}): {', '.join(columns)}")
            except Exception as e:
                typer.echo(f"  {table_name}: Error loading schema - {e}", err=True)

    except InitializationError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

