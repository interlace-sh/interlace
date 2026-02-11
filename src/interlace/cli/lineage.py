"""
CLI command for column-level lineage inspection.

Provides commands to view and analyze column-level data lineage
across models in the pipeline.
"""

import json
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.panel import Panel

from interlace.utils.logging import get_logger

logger = get_logger("interlace.cli.lineage")
console = Console()

app = typer.Typer(
    name="lineage",
    help="Column-level lineage commands",
    no_args_is_help=True,
)


def _load_config(project_dir: Path) -> dict:
    """Load project configuration."""
    import yaml

    config_path = project_dir / "config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f)
    return {}


def _get_lineage_extractor(model_type: str):
    """Get the appropriate lineage extractor for a model type."""
    from interlace.lineage import IbisLineageExtractor, SqlLineageExtractor

    if model_type == "sql":
        return SqlLineageExtractor()
    return IbisLineageExtractor()


def _compute_lineage(model_name: str, models: dict, computed: set = None):
    """
    Compute column lineage for a model.

    Args:
        model_name: Name of the model
        models: All available models
        computed: Set of already computed models (for caching)

    Returns:
        ColumnLineage for the model
    """
    if computed is None:
        computed = set()

    if model_name in computed:
        return None

    if model_name not in models:
        return None

    model_info = models[model_name]
    model_type = model_info.get("type", "python")

    extractor = _get_lineage_extractor(model_type)
    lineage = extractor.extract(model_name, model_info, models)
    computed.add(model_name)

    return lineage


@app.command("show")
def show(
    model: str = typer.Argument(..., help="Model name to show lineage for"),
    column: Optional[str] = typer.Option(
        None, "--column", "-c", help="Specific column to show lineage for"
    ),
    upstream: bool = typer.Option(
        False, "--upstream", "-u", help="Show upstream lineage (sources)"
    ),
    downstream: bool = typer.Option(
        False, "--downstream", "-d", help="Show downstream lineage (dependents)"
    ),
    depth: int = typer.Option(
        3, "--depth", help="Maximum depth to traverse"
    ),
    format: str = typer.Option(
        "tree", "--format", "-f", help="Output format: tree, table, json, dot"
    ),
    project_dir: Path = typer.Option(
        Path("."), "--project", "-p", help="Project directory"
    ),
):
    """
    Show column-level lineage for a model.

    Examples:
        interlace lineage show dim_customer
        interlace lineage show dim_customer --column full_name
        interlace lineage show dim_customer --upstream --depth 5
        interlace lineage show dim_customer --format json
    """
    from interlace.utils.discovery import discover_models
    from interlace.lineage import LineageGraph

    # Load project configuration
    config = _load_config(project_dir)
    models_dir = project_dir / config.get("models_dir", "models")

    if not models_dir.exists():
        console.print(f"[red]Models directory not found: {models_dir}[/red]")
        raise typer.Exit(1)

    # Discover models
    console.print(f"[dim]Discovering models in {models_dir}...[/dim]")
    models = discover_models(models_dir)

    if model not in models:
        console.print(f"[red]Model '{model}' not found[/red]")
        available = sorted(models.keys())[:10]
        if available:
            console.print(f"[dim]Available models: {', '.join(available)}...[/dim]")
        raise typer.Exit(1)

    # Build lineage graph
    console.print("[dim]Computing column lineage...[/dim]")
    graph = LineageGraph()
    computed = set()

    # Compute lineage for target model and dependencies
    def compute_recursive(name: str, direction: str, current_depth: int):
        if current_depth > depth:
            return
        if name in computed:
            return

        if name not in models:
            return

        lineage = _compute_lineage(name, models, computed)
        if lineage:
            graph.add_model_lineage(lineage)

        # Recursively compute for related models
        model_info = models.get(name, {})
        if direction in ("upstream", "both"):
            for dep in model_info.get("dependencies", []):
                compute_recursive(dep, "upstream", current_depth + 1)

    # Determine direction
    if upstream and downstream:
        direction = "both"
    elif upstream:
        direction = "upstream"
    elif downstream:
        direction = "downstream"
    else:
        direction = "both"

    compute_recursive(model, direction, 0)

    # Get lineage data
    model_lineage = graph.models.get(model)
    if model_lineage is None:
        console.print(f"[yellow]No lineage information available for '{model}'[/yellow]")
        raise typer.Exit(0)

    # Output based on format
    if format == "json":
        _output_json(model_lineage, graph, column, upstream, downstream, depth)
    elif format == "table":
        _output_table(model_lineage, graph, column, upstream, downstream, depth)
    elif format == "dot":
        _output_dot(model_lineage, graph, column, upstream, downstream, depth)
    else:  # tree (default)
        _output_tree(model_lineage, graph, column, upstream, downstream, depth)


def _output_tree(lineage, graph, column, upstream, downstream, depth):
    """Output lineage as a tree visualization."""
    from interlace.lineage import ColumnLineage

    model_name = lineage.model_name
    tree = Tree(f"[bold blue]{model_name}[/bold blue]")

    columns_to_show = lineage.columns
    if column:
        col = lineage.get_column(column)
        if col:
            columns_to_show = [col]
        else:
            console.print(f"[yellow]Column '{column}' not found in {model_name}[/yellow]")
            return

    for col_info in columns_to_show:
        col_label = f"[green]{col_info.name}[/green]"
        if col_info.data_type:
            col_label += f" [dim]({col_info.data_type})[/dim]"

        col_branch = tree.add(col_label)

        if col_info.sources:
            sources_branch = col_branch.add("[dim]← sources[/dim]")
            for edge in col_info.sources:
                source_label = f"[cyan]{edge.source_model}[/cyan].[yellow]{edge.source_column}[/yellow]"
                trans_type = edge.transformation_type.value
                source_label += f" [dim][{trans_type}][/dim]"
                src_branch = sources_branch.add(source_label)

                # Recursively show upstream if requested
                if upstream and depth > 1:
                    _add_upstream_tree(
                        src_branch, graph, edge.source_model, edge.source_column, depth - 1
                    )

    console.print(tree)


def _add_upstream_tree(branch, graph, model_name, column_name, remaining_depth):
    """Recursively add upstream lineage to tree."""
    if remaining_depth <= 0:
        return

    edges = graph.get_column_lineage(model_name, column_name)
    for edge in edges:
        source_label = f"[cyan]{edge.source_model}[/cyan].[yellow]{edge.source_column}[/yellow]"
        trans_type = edge.transformation_type.value
        source_label += f" [dim][{trans_type}][/dim]"
        src_branch = branch.add(source_label)

        # Continue recursively
        _add_upstream_tree(
            src_branch, graph, edge.source_model, edge.source_column, remaining_depth - 1
        )


def _output_table(lineage, graph, column, upstream, downstream, depth):
    """Output lineage as a table."""
    table = Table(title=f"Column Lineage: {lineage.model_name}")
    table.add_column("Column", style="green")
    table.add_column("Type", style="dim")
    table.add_column("Source Model", style="cyan")
    table.add_column("Source Column", style="yellow")
    table.add_column("Transformation", style="magenta")

    columns_to_show = lineage.columns
    if column:
        col = lineage.get_column(column)
        if col:
            columns_to_show = [col]

    for col_info in columns_to_show:
        if col_info.sources:
            for i, edge in enumerate(col_info.sources):
                table.add_row(
                    col_info.name if i == 0 else "",
                    col_info.data_type or "" if i == 0 else "",
                    edge.source_model,
                    edge.source_column,
                    edge.transformation_type.value,
                )
        else:
            table.add_row(
                col_info.name,
                col_info.data_type or "",
                "[dim]unknown[/dim]",
                "[dim]unknown[/dim]",
                "",
            )

    console.print(table)


def _output_json(lineage, graph, column, upstream, downstream, depth):
    """Output lineage as JSON."""
    if column:
        col = lineage.get_column(column)
        if col:
            data = col.to_dict()
            if upstream:
                # Add upstream trace
                data["upstream_trace"] = [
                    e.to_dict()
                    for e in graph.trace_column_upstream(lineage.model_name, column, depth)
                ]
        else:
            data = {"error": f"Column '{column}' not found"}
    else:
        data = lineage.to_dict()

    console.print(json.dumps(data, indent=2, default=str))


def _output_dot(lineage, graph, column, upstream, downstream, depth):
    """Output lineage as DOT format for Graphviz."""
    lines = ["digraph lineage {"]
    lines.append("    rankdir=LR;")
    lines.append("    node [shape=box];")

    edges_seen = set()

    def add_edge(src_model, src_col, dst_model, dst_col, trans_type):
        edge_key = (src_model, src_col, dst_model, dst_col)
        if edge_key in edges_seen:
            return
        edges_seen.add(edge_key)

        src_id = f'"{src_model}.{src_col}"'
        dst_id = f'"{dst_model}.{dst_col}"'
        label = trans_type
        lines.append(f"    {src_id} -> {dst_id} [label=\"{label}\"];")

    columns_to_show = lineage.columns
    if column:
        col = lineage.get_column(column)
        if col:
            columns_to_show = [col]

    for col_info in columns_to_show:
        for edge in col_info.sources:
            add_edge(
                edge.source_model,
                edge.source_column,
                edge.output_model,
                edge.output_column or col_info.name,
                edge.transformation_type.value,
            )

    lines.append("}")
    console.print("\n".join(lines))


@app.command("refresh")
def refresh(
    model: Optional[str] = typer.Argument(
        None, help="Model name to refresh (or all if not specified)"
    ),
    project_dir: Path = typer.Option(
        Path("."), "--project", "-p", help="Project directory"
    ),
):
    """
    Refresh/recompute column lineage for models.

    This extracts lineage information from model definitions and
    stores it in the state database.

    Examples:
        interlace lineage refresh                    # Refresh all models
        interlace lineage refresh dim_customer       # Refresh specific model
    """
    from interlace.utils.discovery import discover_models
    from interlace.core.state import StateStore

    # Load project configuration
    config = _load_config(project_dir)
    models_dir = project_dir / config.get("models_dir", "models")

    if not models_dir.exists():
        console.print(f"[red]Models directory not found: {models_dir}[/red]")
        raise typer.Exit(1)

    # Discover models
    console.print("[dim]Discovering models...[/dim]")
    models = discover_models(models_dir)

    # Initialize state store
    state_store = StateStore(config)

    # Determine which models to process
    if model:
        if model not in models:
            console.print(f"[red]Model '{model}' not found[/red]")
            raise typer.Exit(1)
        models_to_process = [model]
    else:
        models_to_process = list(models.keys())

    console.print(f"[dim]Processing {len(models_to_process)} model(s)...[/dim]")

    success_count = 0
    error_count = 0

    for model_name in models_to_process:
        try:
            model_info = models[model_name]
            model_type = model_info.get("type", "python")

            extractor = _get_lineage_extractor(model_type)
            lineage = extractor.extract(model_name, model_info, models)

            # Clear existing lineage for this model
            state_store.clear_model_lineage(model_name)

            # Save columns and lineage edges
            for col in lineage.columns:
                state_store.save_model_column(
                    model_name=model_name,
                    column_name=col.name,
                    data_type=col.data_type,
                    is_nullable=col.nullable,
                    is_primary_key=col.is_primary_key,
                    description=col.description,
                )

                for edge in col.sources:
                    state_store.save_column_lineage(
                        output_model=model_name,
                        output_column=col.name,
                        source_model=edge.source_model,
                        source_column=edge.source_column,
                        transformation_type=edge.transformation_type.value,
                        transformation_expression=edge.transformation_expression,
                        confidence=edge.confidence,
                    )

            success_count += 1
            console.print(f"  [green]✓[/green] {model_name} ({len(lineage.columns)} columns)")

        except Exception as e:
            error_count += 1
            console.print(f"  [red]✗[/red] {model_name}: {e}")
            logger.debug(f"Error processing {model_name}", exc_info=True)

    # Summary
    console.print()
    console.print(
        Panel(
            f"[green]{success_count}[/green] models processed, "
            f"[red]{error_count}[/red] errors",
            title="Lineage Refresh Complete",
        )
    )


@app.command("list")
def list_columns(
    model: str = typer.Argument(..., help="Model name to list columns for"),
    project_dir: Path = typer.Option(
        Path("."), "--project", "-p", help="Project directory"
    ),
):
    """
    List all columns for a model with their lineage summary.

    Examples:
        interlace lineage list dim_customer
    """
    from interlace.utils.discovery import discover_models

    # Load project configuration
    config = _load_config(project_dir)
    models_dir = project_dir / config.get("models_dir", "models")

    if not models_dir.exists():
        console.print(f"[red]Models directory not found: {models_dir}[/red]")
        raise typer.Exit(1)

    # Discover models
    models = discover_models(models_dir)

    if model not in models:
        console.print(f"[red]Model '{model}' not found[/red]")
        raise typer.Exit(1)

    # Compute lineage
    lineage = _compute_lineage(model, models)

    if lineage is None or not lineage.columns:
        console.print(f"[yellow]No column information available for '{model}'[/yellow]")
        raise typer.Exit(0)

    # Display as table
    table = Table(title=f"Columns: {model}")
    table.add_column("Column", style="green")
    table.add_column("Type", style="dim")
    table.add_column("Sources", style="cyan")

    for col in lineage.columns:
        source_count = len(col.sources)
        if source_count == 0:
            sources_str = "[dim]unknown[/dim]"
        elif source_count == 1:
            src = col.sources[0]
            sources_str = f"{src.source_model}.{src.source_column}"
        else:
            sources_str = f"{source_count} sources"

        table.add_row(
            col.name,
            col.data_type or "",
            sources_str,
        )

    console.print(table)
