"""Scaffold a new interlace project (used by ``interlace init``).

Templates are on-disk project trees under ``templates/<name>/`` — each a real,
runnable project plus a ``template.yaml`` (metadata, not copied) and a README that
doubles as its landing page. ``interlace init`` copies a chosen template into the
target directory, substituting the project name. The default, ``quickstart``, is a
no-source SQL → Python → SQL chain; source templates (GitHub, Postgres, …) scaffold
a real extraction against :mod:`interlace.sources`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from interlace.config.config import CONFIG_FILE
from interlace.exceptions import ConfigurationError

TEMPLATES_DIR = Path(__file__).parent / "templates"
_META_FILE = "template.yaml"
_NAME_TOKEN = "__PROJECT_NAME__"  # replaced with the project name in every template file
DEFAULT_TEMPLATE = "quickstart"


@dataclass(frozen=True)
class TemplateInfo:
    """A scaffoldable template: its directory name, human title, one-line blurb,
    and any environment variables its models expect (surfaced as next-steps)."""

    name: str
    title: str
    description: str
    requires_env: tuple[str, ...] = ()


def _load_meta(directory: Path) -> TemplateInfo:
    meta = yaml.safe_load((directory / _META_FILE).read_text()) or {}
    return TemplateInfo(
        name=directory.name,
        title=str(meta.get("title", directory.name)),
        description=str(meta.get("description", "")),
        requires_env=tuple(meta.get("requires_env", []) or []),
    )


def list_templates() -> list[TemplateInfo]:
    """Every available template (any ``templates/`` subdirectory with a
    ``template.yaml``), the default first, then the rest alphabetically."""
    found = [_load_meta(d) for d in TEMPLATES_DIR.iterdir() if d.is_dir() and (d / _META_FILE).exists()]
    return sorted(found, key=lambda t: (t.name != DEFAULT_TEMPLATE, t.name))


def _resolve_template(name: str) -> Path:
    directory = TEMPLATES_DIR / name
    if not (directory / _META_FILE).exists():
        available = ", ".join(t.name for t in list_templates())
        raise ConfigurationError(f"unknown template {name!r}; available: {available}", details={"template": name})
    return directory


def scaffold_project(root: Path, name: str | None = None, template: str = DEFAULT_TEMPLATE) -> list[Path]:
    """Copy ``template`` into ``root`` as a new project, substituting the project
    name. Refuses to overwrite an existing project. Returns the files written."""
    root = Path(root)
    project_name = name or root.resolve().name
    source = _resolve_template(template)

    if (root / CONFIG_FILE).exists():
        raise ConfigurationError("project already initialised", details={"path": str(root / CONFIG_FILE)})

    written: list[Path] = []
    for item in sorted(source.rglob("*")):
        if item.name == _META_FILE or not item.is_file() or _is_install_artefact(item):
            continue
        target = root / item.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            text = item.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            # A template may legitimately carry a binary fixture (parquet, sqlite).
            target.write_bytes(item.read_bytes())
        else:
            target.write_text(text.replace(_NAME_TOKEN, project_name), encoding="utf-8")
        written.append(target)
    return written


def _is_install_artefact(item: Path) -> bool:
    """Templates are real projects, so they contain ``.py`` model files — and pip
    byte-compiles every ``.py`` in the wheel at install time. That leaves
    ``__pycache__/*.pyc`` sitting inside the *installed* template tree, which is an
    artefact of the install and never part of the template. Copying one into a new
    project is wrong, and reading it as text raised ``UnicodeDecodeError`` on the
    first command a pip user ever ran. (uv does not byte-compile by default, which
    is why this only reproduced via pip.)"""
    return "__pycache__" in item.parts or item.suffix in {".pyc", ".pyo"}
