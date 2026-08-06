"""Error text is data, not Rich markup.

An error naming an extra — ``pip install 'interlaced[adbc]'`` — is interpolated
into a ``[red]...[/red]`` string on its way to the console. Rich parses the
``[adbc]`` as a style tag and drops it, so the message rendered as ``pip install
'interlaced'``: it told the user to install exactly what they already had. Every
extra-suggesting error in the codebase had this, on the first run, in the first
minute.
"""

from __future__ import annotations

import inspect
import io
from pathlib import Path

import pytest
from rich.console import Console
from rich.markup import escape

import interlace.cli.main as cli_main
from interlace.exceptions import ConfigurationError

pytestmark = pytest.mark.unit

# Resolved from the imported module, not the working directory, so the
# source-scanning tests below hold wherever pytest is invoked from.
CLI_SOURCE = Path(inspect.getsourcefile(cli_main) or "").read_text()


def _render(markup: str) -> str:
    console = Console(file=io.StringIO(), force_terminal=False, width=200)
    console.print(markup)
    return console.file.getvalue().strip()  # type: ignore[attr-defined]


# Every extra a user can be told to install. Each is a valid Rich style-tag
# spelling, so each is silently eaten without the escape.
EXTRAS = ["service", "adbc", "spark", "adbc-snowflake", "adbc-bigquery", "all", "polars"]


@pytest.mark.parametrize("extra", EXTRAS)
def test_extra_survives_rendering(extra: str) -> None:
    exc = ConfigurationError(f"needs the '{extra}' extra: pip install 'interlaced[{extra}]'")

    # The rendering main() performs.
    assert f"interlaced[{extra}]" in _render(f"[red]error:[/red] {escape(exc.message)}")


@pytest.mark.parametrize("extra", EXTRAS)
def test_unescaped_rendering_still_loses_it(extra: str) -> None:
    """Pins the reason the escape is required, so removing it fails loudly."""
    exc = ConfigurationError(f"pip install 'interlaced[{extra}]'")

    assert f"interlaced[{extra}]" not in _render(f"[red]error:[/red] {exc.message}")


def test_cli_interpolates_error_text_through_escape() -> None:
    """No unescaped ``exc.message`` may reach a markup string in the CLI."""
    offenders = [line.strip() for line in CLI_SOURCE.splitlines() if "exc.message" in line and "escape(" not in line]
    assert offenders == [], f"unescaped error text in markup: {offenders}"


def test_serve_hint_keeps_its_extra() -> None:
    """The one literal that carries an extra inline rather than via an exception."""
    assert r"interlaced\[service]" in CLI_SOURCE
