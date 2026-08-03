"""dbt-style model selection.

A selector is one of: ``model`` (exact), ``+model`` (model + ancestors),
``model+`` (model + descendants), ``+model+`` (both), ``tag:<name>``, or
``state:modified`` (models whose fingerprint differs from the promoted one —
requires the caller to pass the environment's mapping; supports the same
``+``/``+…+`` affixes, e.g. ``state:modified+`` for changed models and
everything downstream). Multiple selectors (space- or comma-separated, or
repeated) union together. An empty selector list means "all models".
"""

from __future__ import annotations

from interlace.exceptions import SelectionError
from interlace.graph.project import CompiledProject


def select_models(
    selectors: list[str], project: CompiledProject, *, promoted: dict[str, str] | None = None
) -> set[str]:
    """Resolve ``selectors`` to model names. ``promoted`` is the target
    environment's model->fingerprint mapping, needed only by ``state:modified``."""
    if not selectors:
        return set(project.models)
    chosen: set[str] = set()
    for raw in selectors:
        for token in _tokens(raw):
            chosen |= _resolve(token, project, promoted)
    return chosen


def wants_state(selectors: list[str]) -> bool:
    """Whether any selector needs the environment mapping (``state:...``)."""
    return any(token.strip("+").startswith("state:") for raw in selectors for token in _tokens(raw))


def _tokens(raw: str) -> list[str]:
    return [token for token in raw.replace(",", " ").split() if token]


def _resolve(token: str, project: CompiledProject, promoted: dict[str, str] | None) -> set[str]:
    include_ancestors = token.startswith("+")
    include_descendants = token.endswith("+")
    core = token.strip("+")

    if core.startswith("tag:"):
        tag = core[4:]
        matched = {name for name, model in project.models.items() if tag in model.tags}
        if not matched:
            # a vacuous match turns a CI gate into a no-op that "passes"
            known = sorted({t for model in project.models.values() for t in model.tags})
            raise SelectionError(
                f"tag {tag!r} matches no models" + (f" (known tags: {', '.join(known)})" if known else ""),
                details={"selector": token},
            )
        return _spread(matched, project, include_ancestors, include_descendants)

    if core.startswith("state:"):
        kind = core[6:]
        if kind != "modified":
            raise SelectionError(
                f"unknown state selector {kind!r}; only state:modified exists", details={"selector": token}
            )
        if promoted is None:
            raise SelectionError(
                "state:modified needs the target environment's promoted fingerprints; "
                "this entry point cannot supply them",
                details={"selector": token},
            )
        matched = {name for name, model in project.models.items() if promoted.get(name) != model.fingerprint}
        # empty is legitimate here: "nothing changed" must select nothing, not error
        return _spread(matched, project, include_ancestors, include_descendants)

    name = core
    if name not in project.models:
        raise SelectionError(f"unknown model in selector: {name!r}", details={"selector": token})
    return _spread({name}, project, include_ancestors, include_descendants)


def _spread(names: set[str], project: CompiledProject, include_ancestors: bool, include_descendants: bool) -> set[str]:
    chosen = set(names)
    for name in names:
        if include_ancestors:
            chosen |= project.graph.ancestors(name)
        if include_descendants:
            chosen |= project.graph.descendants(name)
    return chosen
