"""dbt-style model selection."""

from __future__ import annotations

import pytest

from interlace.dsl.decorators import ModelDef
from interlace.exceptions import SelectionError
from interlace.graph.project import compile_models
from interlace.graph.selectors import select_models

pytestmark = pytest.mark.unit


def _project() -> object:
    return compile_models(
        [
            ModelDef(name="a", sql="SELECT 1 AS x", tags=("base",)),
            ModelDef(name="b", sql="SELECT x FROM a"),
            ModelDef(name="c", sql="SELECT x FROM b", tags=("finance",)),
        ]
    )


def test_transitive_ancestors_and_descendants() -> None:
    graph = _project().graph  # type: ignore[attr-defined]
    assert graph.ancestors("c") == {"a", "b"}
    assert graph.descendants("a") == {"b", "c"}
    assert graph.ancestors("a") == set()


def test_empty_selection_is_all() -> None:
    assert select_models([], _project()) == {"a", "b", "c"}  # type: ignore[arg-type]


def test_exact_and_graph_operators() -> None:
    project = _project()
    assert select_models(["b"], project) == {"b"}  # type: ignore[arg-type]
    assert select_models(["+c"], project) == {"a", "b", "c"}  # type: ignore[arg-type]
    assert select_models(["a+"], project) == {"a", "b", "c"}  # type: ignore[arg-type]
    assert select_models(["+b+"], project) == {"a", "b", "c"}  # type: ignore[arg-type]


def test_tag_and_multiple_selectors() -> None:
    project = _project()
    assert select_models(["tag:finance"], project) == {"c"}  # type: ignore[arg-type]
    assert select_models(["a", "c"], project) == {"a", "c"}  # type: ignore[arg-type]
    assert select_models(["a,c"], project) == {"a", "c"}  # comma-separated  # type: ignore[arg-type]


def test_unknown_selector_raises() -> None:
    with pytest.raises(SelectionError):
        select_models(["nope"], _project())  # type: ignore[arg-type]


def test_state_modified_selects_drifted_models() -> None:
    from interlace.dsl.decorators import ModelDef
    from interlace.graph.project import compile_models
    from interlace.graph.selectors import select_models, wants_state

    v1 = compile_models(
        [
            ModelDef(name="a", sql="SELECT 1 AS x"),
            ModelDef(name="b", sql="SELECT x FROM a"),
            ModelDef(name="c", sql="SELECT 9 AS z"),
        ]
    )
    promoted = {name: model.fingerprint for name, model in v1.models.items()}  # what apply recorded
    compiled = compile_models(  # then a's SQL changed: b drifts transitively, c does not
        [
            ModelDef(name="a", sql="SELECT 2 AS x"),
            ModelDef(name="b", sql="SELECT x FROM a"),
            ModelDef(name="c", sql="SELECT 9 AS z"),
        ]
    )

    assert wants_state(["state:modified"]) and wants_state(["state:modified+"]) and not wants_state(["a+"])
    changed = select_models(["state:modified"], compiled, promoted=promoted)
    assert changed == {"a", "b"}  # b's fingerprint hashes a's, so it drifts too
    assert select_models(["state:modified+"], compiled, promoted=promoted) == {"a", "b"}
    # nothing drifted -> empty selection, NOT an error (a CI no-op must be a no-op)
    clean = {name: model.fingerprint for name, model in compiled.models.items()}
    assert select_models(["state:modified"], compiled, promoted=clean) == set()

    import pytest as _pytest

    from interlace.exceptions import SelectionError

    with _pytest.raises(SelectionError, match="cannot supply"):
        select_models(["state:modified"], compiled)
    with _pytest.raises(SelectionError, match="only state:modified"):
        select_models(["state:fresh"], compiled, promoted=clean)
