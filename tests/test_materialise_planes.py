"""The materialisation reframe: virtual / view / ephemeral (interlace-owned) vs
table / file (terminal). Covers the plane-aware resolver, the compile-time guards,
and terminal contract validation."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import fetch_rows as _rows

from interlace.dsl.decorators import ModelDef, model, validate_materialise
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import DefinitionError, PlanError, SchemaError
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.state.store import SqliteStateStore
from interlace.strategies import Append, Merge, Replace, ReplaceInPlace, View, resolve_strategy

pytestmark = pytest.mark.unit


def test_resolve_strategy_plane_matrix() -> None:
    # full differs by ownership: virtual rewrites the whole table, table replaces in place
    assert isinstance(resolve_strategy("virtual", "replace"), Replace)
    assert isinstance(resolve_strategy("table", "replace"), ReplaceInPlace)
    assert isinstance(resolve_strategy("view", "replace"), View)
    # append is external-only; keyed builders are shared across virtual/table
    assert isinstance(resolve_strategy("table", "append"), Append)
    assert isinstance(resolve_strategy("table", "merge", ("id",)), Merge)
    assert isinstance(resolve_strategy("virtual", "merge", ("id",)), Merge)
    with pytest.raises(PlanError, match="append requires materialise: table"):
        resolve_strategy("virtual", "append")


def test_default_materialise_is_virtual() -> None:
    assert ModelDef(name="m", sql="SELECT 1 AS id").materialise == "virtual"
    assert ModelDef(name="m", sql="SELECT 1 AS id").is_terminal is False
    assert ModelDef(name="m", sql="SELECT 1", materialise="table", target="ext.t").is_terminal is True


def test_python_model_cannot_be_terminal() -> None:
    with pytest.raises(DefinitionError, match="cannot materialise as 'table'"):

        @model(name="p", materialise="table")
        def _p() -> None: ...


def test_python_model_export_kwarg_raises_migration_error() -> None:
    with pytest.raises(DefinitionError, match="export= was removed in 2.0"):

        @model(name="p", export={"to": "table", "target": "ext.t"})
        def _p() -> None: ...


async def test_depending_on_a_table_is_allowed_but_a_file_is_not() -> None:
    # a file isn't a readable table
    with pytest.raises(DefinitionError, match="a file isn't a readable"):
        compile_models(
            [
                ModelDef(name="dump", sql="SELECT 1 AS id", materialise="file", format="csv", path="o.csv"),
                ModelDef(name="downstream", sql="SELECT id FROM dump"),
            ]
        )
    # depending on a table is fine — it's read via its delivered external target
    compiled = compile_models(
        [
            ModelDef(name="push", sql="SELECT 1 AS id", materialise="table", target="ext.main.t"),
            ModelDef(name="downstream", sql="SELECT id FROM push"),
        ]
    )
    assert "push" in compiled.models["downstream"].dependencies


async def test_cross_engine_table_dependency_is_rejected() -> None:
    with pytest.raises(DefinitionError, match="cross-engine"):
        compile_models(
            [
                ModelDef(name="push", sql="SELECT 1 AS id", materialise="table", target="ext.main.t", engine="other"),
                ModelDef(name="downstream", sql="SELECT id FROM push"),
            ],
            known_engines={"default", "other"},
        )


async def test_file_rejects_checks_but_table_allows_them() -> None:
    from interlace.checks.spec import CheckSpec

    check = (CheckSpec(type="not_null", columns=("id",), severity="error", params={}),)
    # a file has no queryable relation to check
    with pytest.raises(DefinitionError, match="no queryable table to check"):
        compile_models(
            [ModelDef(name="dump", sql="SELECT 1 AS id", materialise="file", format="csv", path="o.csv", checks=check)]
        )
    # a table delivers into a real external table, so it CAN carry checks
    compiled = compile_models(
        [ModelDef(name="push", sql="SELECT 1 AS id", materialise="table", target="ext.main.t", checks=check)]
    )
    assert compiled.models["push"].checks == check


async def test_terminal_table_contract_validated(tmp_path: Path) -> None:
    """A columns: contract on a terminal table is validated against the delivered
    external table; a type it can't satisfy fails the apply."""
    engine = DuckDBAdapter.in_memory()
    engine.attach("ext", ":memory:")
    store = await SqliteStateStore.open(tmp_path / "s.db")
    try:
        ok = compile_models(
            [
                ModelDef(
                    name="push",
                    sql="SELECT 1 AS id",
                    materialise="table",
                    target="ext.main.contracted",
                    columns={"id": "INTEGER"},
                )
            ]
        )
        await apply(await diff(ok, "prod", store), compiled=ok, engine=engine, state=store)
        assert await _rows(engine, "SELECT id FROM ext.main.contracted") == [{"id": 1}]

        bad = compile_models(
            [
                ModelDef(
                    name="push2",
                    sql="SELECT 'x' AS id",
                    materialise="table",
                    target="ext.main.bad",
                    columns={"id": "INTEGER"},
                )
            ]
        )
        with pytest.raises(SchemaError):
            await apply(await diff(bad, "prod", store), compiled=bad, engine=engine, state=store)
    finally:
        await store.close()
        engine.close()


def test_validate_materialise_accepts_valid_configs() -> None:
    validate_materialise("m", materialise="virtual", strategy="replace", target=None, path=None, format=None, key=())
    validate_materialise(
        "m", materialise="table", strategy="merge", target="ext.t", path=None, format=None, key=("id",)
    )
    validate_materialise("m", materialise="file", strategy="replace", target=None, path="o.csv", format="csv", key=())
