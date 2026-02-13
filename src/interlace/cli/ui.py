"""
interlace ui - UI status commands.

Check the status of the embedded web UI.
"""

from pathlib import Path

import typer

app = typer.Typer(name="ui", help="Web UI information")


def get_static_dir() -> Path:
    """Get the static directory for the service."""
    return Path(__file__).parent.parent / "service" / "static"


@app.command()
def status() -> None:
    """
    Check the status of the embedded UI.
    """
    static_dir = get_static_dir()

    if not static_dir.exists():
        typer.echo("UI Status: Not installed")
        typer.echo(f"  Static directory does not exist: {static_dir}")
        return

    index_file = static_dir / "index.html"
    assets_dir = static_dir / "assets"

    if not index_file.exists():
        typer.echo("UI Status: Incomplete")
        typer.echo(f"  Missing index.html in {static_dir}")
        return

    # Count assets
    asset_count = 0
    total_size = 0
    if assets_dir.exists():
        for f in assets_dir.rglob("*"):
            if f.is_file():
                asset_count += 1
                total_size += f.stat().st_size

    typer.echo("UI Status: Installed")
    typer.echo(f"  Location: {static_dir}")
    typer.echo(f"  Assets: {asset_count} files ({total_size / 1024:.1f} KB)")
