"""Compiling models into a fingerprinted dependency graph."""

from __future__ import annotations

import pytest

from interlace.dsl.decorators import ModelDef
from interlace.graph.project import compile_models

pytestmark = pytest.mark.unit


def sql_model(name: str, sql: str, **kwargs: object) -> ModelDef:
    return ModelDef(name=name, sql=sql, **kwargs)  # type: ignore[arg-type]


def test_extracts_sql_dependencies_and_orders_upstreams_first() -> None:
    models = [
        sql_model("orders_enriched", "SELECT * FROM orders JOIN customers USING (cid)"),
        sql_model("orders", "SELECT 1 AS id, 2 AS cid"),
        sql_model("customers", "SELECT 2 AS cid"),
    ]
    project = compile_models(models)

    assert set(project.models["orders_enriched"].dependencies) == {"orders", "customers"}
    order = [m.name for m in project.ordered()]
    assert order.index("orders") < order.index("orders_enriched")
    assert order.index("customers") < order.index("orders_enriched")


def test_external_tables_are_not_dependencies() -> None:
    project = compile_models([sql_model("orders", "SELECT * FROM raw_source")])
    assert project.models["orders"].dependencies == ()


def test_cte_names_are_not_dependencies() -> None:
    project = compile_models([sql_model("m", "WITH t AS (SELECT 1 AS x) SELECT * FROM t")])
    assert project.models["m"].dependencies == ()


def test_qualified_reference_matches_model_by_tail() -> None:
    models = [sql_model("orders", "SELECT 1 AS id"), sql_model("agg", "SELECT * FROM main.orders")]
    project = compile_models(models)
    assert project.models["agg"].dependencies == ("orders",)


def test_fingerprint_is_deterministic_and_sql_sensitive() -> None:
    a = compile_models([sql_model("m", "SELECT 1 AS x")]).models["m"].fingerprint
    b = compile_models([sql_model("m", "SELECT 1 AS x")]).models["m"].fingerprint
    c = compile_models([sql_model("m", "SELECT 2 AS x")]).models["m"].fingerprint
    assert a == b
    assert a != c


def test_upstream_change_propagates_to_downstream_fingerprint() -> None:
    def downstream_fp(upstream_sql: str) -> str:
        project = compile_models([sql_model("up", upstream_sql), sql_model("down", "SELECT * FROM up")])
        return project.models["down"].fingerprint

    assert downstream_fp("SELECT 1 AS x") != downstream_fp("SELECT 2 AS x")


def test_physical_table_embeds_schema_and_fingerprint() -> None:
    model = compile_models([sql_model("silver.orders", "SELECT 1 AS id")]).models["silver.orders"]
    assert model.physical_table.schema == "interlace__silver"
    assert model.physical_table.name == f"orders__{model.fingerprint}"


def test_cycle_in_models_is_rejected() -> None:
    from interlace.exceptions import DependencyError

    models = [sql_model("a", "SELECT * FROM b"), sql_model("b", "SELECT * FROM a")]
    with pytest.raises(DependencyError):
        compile_models(models)
