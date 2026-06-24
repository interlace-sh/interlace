"""dbt-style model selection.

A selector is one of: ``model`` (exact), ``+model`` (model + ancestors),
``model+`` (model + descendants), ``+model+`` (both), or ``tag:<name>``. Multiple
selectors (space- or comma-separated, or repeated) union together. An empty
selector list means "all models".
"""

from __future__ import annotations

from interlace.exceptions import SelectionError
from interlace.graph.project import CompiledProject


def select_models(selectors: list[str], project: CompiledProject) -> set[str]:
    if not selectors:
        return set(project.models)
    chosen: set[str] = set()
    for raw in selectors:
        for token in _tokens(raw):
            chosen |= _resolve(token, project)
    return chosen


def _tokens(raw: str) -> list[str]:
    return [token for token in raw.replace(",", " ").split() if token]


def _resolve(token: str, project: CompiledProject) -> set[str]:
    if token.startswith("tag:"):
        tag = token[4:]
        return {name for name, model in project.models.items() if tag in model.tags}

    include_ancestors = token.startswith("+")
    include_descendants = token.endswith("+")
    name = token.strip("+")
    if name not in project.models:
        raise SelectionError(f"unknown model in selector: {name!r}", details={"selector": token})

    chosen = {name}
    if include_ancestors:
        chosen |= project.graph.ancestors(name)
    if include_descendants:
        chosen |= project.graph.descendants(name)
    return chosen
