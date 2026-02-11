"""
CLI command for impact analysis / plan preview.

Shows what will change before execution, detecting breaking changes
and affected downstream models.
"""

import json
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from interlace.utils.logging import get_logger

logger = get_logger("interlace.cli.plan")
console = Console()

app = typer.Typer(
    name="plan",
    help="Preview execution plan and impact analysis",
    no_args_is_help=False,
)


def _load_config(project_dir: Path) -> dict:
    """Load project configuration."""
    import yaml

    config_path = project_dir / "config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f)
    return {}


@app.callback(invoke_without_command=True)
def plan(
    ctx: typer.Context,
    models: Optional[List[str]] = typer.Argument(
        None, help="Specific models to analyze (default: all)"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Treat all models as needing to run"
    ),
    format: str = typer.Option(
        "table", "--format", help="Output format: table, json, summary"
    ),
    show_schema: bool = typer.Option(
        False, "--schema", "-s", help="Show detailed schema changes"
    ),
    project_dir: Path = typer.Option(
        Path("."), "--project", "-p", help="Project directory"
    ),
):
    """
    Preview what will happen during execution.

    Analyzes which models will run, why they need to run,
    and what downstream models will be affected.

    Examples:
        interlace plan                         # Analyze all models
        interlace plan customer_staging        # Analyze specific model
        interlace plan --force                 # Show plan for full run
        interlace plan --format json           # JSON output
    """
    if ctx.invoked_subcommand is not None:
        return

    from interlace.utils.discovery import discover_models
    from interlace.core.deps import DependencyGraph
    from interlace.core.impact import ImpactAnalyzer
    from interlace.core.state import StateStore

    # Load project configuration
    config = _load_config(project_dir)
    models_dir = project_dir / config.get("models_dir", "models")

    if not models_dir.exists():
        console.print(f"[red]Models directory not found: {models_dir}[/red]")
        raise typer.Exit(1)

    # Discover models
    console.print("[dim]Discovering models...[/dim]")
    all_models = discover_models(models_dir)

    if not all_models:
        console.print("[yellow]No models found[/yellow]")
        raise typer.Exit(0)

    # Build dependency graph
    graph = DependencyGraph(all_models)

    # Initialize state store
    state_store = StateStore(config)

    # Try to get change detector
    change_detector = None
    try:
        from interlace.core.execution.change_detector import ChangeDetector

        change_detector = ChangeDetector(all_models, state_store)
    except Exception as e:
        logger.debug(f"Could not initialize change detector: {e}")

    # Create impact analyzer
    analyzer = ImpactAnalyzer(
        models=all_models,
        graph=graph,
        change_detector=change_detector,
        state_store=state_store,
    )

    # Analyze
    target_models = models if models else None
    console.print("[dim]Analyzing impact...[/dim]")
    result = analyzer.analyze(target_models=target_models, force=force)

    # Output based on format
    if format == "json":
        console.print(json.dumps(result.to_dict(), indent=2, default=str))
    elif format == "summary":
        _output_summary(result, show_schema)
    else:
        _output_table(result, show_schema)


def _output_table(result, show_schema: bool):
    """Output impact analysis as a table."""
    from interlace.core.impact import RunReason

    # Models to run table
    if result.models_to_run:
        table = Table(title="Models to Execute", title_style="bold")
        table.add_column("Model", style="cyan")
        table.add_column("Reason", style="yellow")
        table.add_column("Downstream Affected", style="dim")
        table.add_column("Changes", style="magenta")

        reason_labels = {
            RunReason.FILE_CHANGED: "file changed",
            RunReason.UPSTREAM_CHANGED: "upstream changed",
            RunReason.FIRST_RUN: "first run",
            RunReason.FORCE: "forced",
            RunReason.CONFIG_CHANGED: "config changed",
            RunReason.SCHEMA_CHANGED: "schema changed",
        }

        for impact in result.models_to_run:
            reason = reason_labels.get(impact.run_reason, str(impact.run_reason))

            downstream_str = ""
            if impact.downstream_affected:
                if len(impact.downstream_affected) <= 3:
                    downstream_str = ", ".join(impact.downstream_affected)
                else:
                    downstream_str = f"{len(impact.downstream_affected)} models"

            changes_str = ""
            if impact.schema_changes:
                added = sum(1 for c in impact.schema_changes if c.change_type == "added")
                removed = sum(1 for c in impact.schema_changes if c.change_type == "removed")
                changed = sum(1 for c in impact.schema_changes if c.change_type == "type_changed")
                parts = []
                if added:
                    parts.append(f"+{added}")
                if removed:
                    parts.append(f"-{removed}")
                if changed:
                    parts.append(f"~{changed}")
                changes_str = " ".join(parts)

            table.add_row(
                impact.model_name,
                reason,
                downstream_str or "-",
                changes_str or "-",
            )

        console.print(table)

    # Schema changes detail
    if show_schema:
        for impact in result.models_to_run:
            if impact.schema_changes:
                console.print()
                console.print(f"[bold]Schema Changes: {impact.model_name}[/bold]")
                for change in impact.schema_changes:
                    if change.change_type == "added":
                        console.print(f"  [green]+[/green] {change.column_name} ({change.new_type})")
                    elif change.change_type == "removed":
                        console.print(f"  [red]-[/red] {change.column_name} ({change.old_type})")
                    else:
                        console.print(
                            f"  [yellow]~[/yellow] {change.column_name}: "
                            f"{change.old_type} → {change.new_type}"
                        )

    # Breaking changes
    if result.breaking_changes:
        console.print()
        console.print("[bold red]Breaking Changes:[/bold red]")
        for bc in result.breaking_changes:
            severity_color = "yellow" if bc.severity == "warning" else "red"
            console.print(f"  [{severity_color}]⚠[/{severity_color}] {bc.description}")
            if bc.affected_downstream:
                affected = ", ".join(bc.affected_downstream[:3])
                if len(bc.affected_downstream) > 3:
                    affected += f" (+{len(bc.affected_downstream) - 3} more)"
                console.print(f"    Affected: {affected}")

    # Summary
    console.print()
    summary = result.to_dict()["summary"]
    summary_text = Text()
    summary_text.append("Summary: ", style="bold")
    summary_text.append(f"{summary['will_run']} models will run", style="green")
    if summary["skipped"]:
        summary_text.append(f", {summary['skipped']} skipped", style="dim")
    if summary["warnings"]:
        summary_text.append(f", {summary['warnings']} warnings", style="yellow")
    if summary["errors"]:
        summary_text.append(f", {summary['errors']} errors", style="red")

    console.print(Panel(summary_text, title="Plan Complete"))


def _output_summary(result, show_schema: bool):
    """Output a brief summary."""
    console.print()

    if result.models_to_run:
        console.print(f"[green]✓[/green] {len(result.models_to_run)} models will run:")
        for impact in result.models_to_run[:10]:
            reason = impact.run_reason.value if impact.run_reason else "unknown"
            downstream_count = len(impact.downstream_affected)
            downstream_str = f" → {downstream_count} downstream" if downstream_count else ""
            console.print(f"  • {impact.model_name} ({reason}){downstream_str}")
        if len(result.models_to_run) > 10:
            console.print(f"  ... and {len(result.models_to_run) - 10} more")

    if result.models_skipped:
        console.print()
        console.print(f"[dim]⊘ {len(result.models_skipped)} models skipped (no changes)[/dim]")

    if result.breaking_changes:
        console.print()
        console.print(f"[yellow]⚠ {len(result.breaking_changes)} breaking change(s) detected[/yellow]")
        for bc in result.breaking_changes[:5]:
            console.print(f"  • {bc.description}")

    console.print()
    console.print(f"Total affected: {result.total_affected} models")
