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
