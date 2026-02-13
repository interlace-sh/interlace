"""
interlace init - Project initialization.

Phase 0: Create new Interlace project structure.
"""

from pathlib import Path

import typer

app = typer.Typer(name="init", help="Create a new Interlace project", invoke_without_command=True)


@app.callback()
def init(
    ctx: typer.Context,
    project_name: str = typer.Argument(None, help="Project name"),
    template: str = typer.Option("default", help="Project template"),
):
    """
    Initialize a new Interlace project.

    Phase 0: Create basic project structure with config.yaml and models/ directory.
    """
    if ctx.invoked_subcommand is None:
        if not project_name:
            project_name = "interlace-project"

        project_path = Path(project_name)

        if project_path.exists():
            typer.echo(f"Error: Directory {project_name} already exists", err=True)
            raise typer.Exit(1)

        # Create directory structure
        project_path.mkdir()
        (project_path / "models").mkdir()
        (project_path / "tests").mkdir()
        (project_path / "functions").mkdir()

        # Create config.yaml
        config_content = f"""# Interlace Configuration
# Base configuration shared across all environments

name: {project_name}
description: Interlace project

connections:
  duckdb_main:
    type: duckdb
    path: data/{{env}}/main.duckdb  # {{env}} replaced at runtime
    attach: []

scheduler:
  engine: aioclock
  timezone: UTC
  tick_ms: 500

# Global model defaults
models:
  default_schema: public
  default_materialize: table
"""

        (project_path / "config.yaml").write_text(config_content)

        # Create .gitignore
        gitignore_content = """# Interlace
data/
*.db
*.duckdb
__pycache__/
*.pyc
.venv/
.env
.env.*
!config.yaml
!config.*.yaml
"""
        (project_path / ".gitignore").write_text(gitignore_content)

        # Create example model
        example_model = """# models/example.py
from interlace import model
import ibis

@model(
    name="example",
    schema="public",
    materialize="table",
    strategy="append"
)
def example():
    \"\"\"Example model - replace with your own.\"\"\"
    con = ibis.duckdb.connect()
    return con.create_table(
        [{"id": 1, "name": "Example 1"}, {"id": 2, "name": "Example 2"}]
    )
"""
        (project_path / "models" / "example.py").write_text(example_model)

        typer.echo(f"Created Interlace project: {project_name}")
        typer.echo(f"  - Configuration: {project_name}/config.yaml")
        typer.echo(f"  - Models: {project_name}/models/")
        typer.echo(f"  - Tests: {project_name}/tests/")
        typer.echo(f"  - Functions: {project_name}/functions/")
