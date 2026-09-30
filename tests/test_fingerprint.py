"""Snapshot fingerprinting determinism and sensitivity."""

from __future__ import annotations

import inspect
import textwrap

import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef
from interlace.graph.project import compile_models
from interlace.ir.fingerprint import data_fingerprint, metadata_fingerprint, python_source

pytestmark = pytest.mark.unit


def fp(query: str, *, strategy: dict | None = None, upstreams: list[str] | None = None) -> str:
    return data_fingerprint(
        query=sqlglot.parse_one(query),
        strategy_config=strategy or {"strategy": "replace"},
        upstream_fingerprints=upstreams or [],
    )


def test_fingerprint_is_deterministic() -> None:
    assert fp("SELECT 1") == fp("SELECT 1")


def test_query_change_changes_fingerprint() -> None:
    assert fp("SELECT 1") != fp("SELECT 2")


def test_strategy_config_change_changes_fingerprint() -> None:
    assert fp("SELECT 1", strategy={"strategy": "replace"}) != fp("SELECT 1", strategy={"strategy": "merge"})


def test_upstream_change_propagates() -> None:
    assert fp("SELECT 1", upstreams=["aaaa"]) != fp("SELECT 1", upstreams=["bbbb"])


def test_upstream_order_is_irrelevant() -> None:
    assert fp("SELECT 1", upstreams=["aaaa", "bbbb"]) == fp("SELECT 1", upstreams=["bbbb", "aaaa"])


def test_metadata_fingerprint_is_independent_of_data() -> None:
    assert metadata_fingerprint({"owner": "alice"}) != metadata_fingerprint({"owner": "bob"})


def test_a_computed_default_is_part_of_the_python_fingerprint() -> None:
    def orders(raw: object, n: int = 1 + 1) -> int:
        return n

    assert python_source(orders) != textwrap.dedent(inspect.getsource(orders))


def test_literal_default_stays_out_of_the_python_fingerprint() -> None:
    def plain(cursor: object = None) -> object:
        return cursor

    assert python_source(plain) == textwrap.dedent(inspect.getsource(plain))


def test_factory_default_changes_the_python_fingerprint() -> None:
    def make(tenant: str):
        def orders(raw: object, tenant: str = tenant) -> str:
            return tenant

        return orders

    acme = python_source(make("acme"))
    assert acme != python_source(make("globex"))
    assert acme == python_source(make("acme"))


def test_closure_cell_changes_the_python_fingerprint() -> None:
    def make(tenant: str):
        def orders(raw: object) -> str:
            return tenant

        return orders

    assert python_source(make("acme")) != python_source(make("globex"))


def test_mutated_default_changes_the_python_fingerprint() -> None:
    def holder():
        items: list[str] = []

        def orders(raw: object, items: list[str] = items) -> list[str]:
            return items

        return orders

    fn = holder()
    before = python_source(fn)
    fn.__defaults__[0].append("x")  # type: ignore[union-attr]
    assert python_source(fn) != before


def test_non_data_capture_is_hashed_by_type() -> None:
    class Client:
        pass

    def make(client: Client):
        def orders(raw: object, client: Client = client) -> Client:
            return client

        return orders

    assert python_source(make(Client())) == python_source(make(Client()))


def test_generated_models_plan_apart_when_the_captured_value_differs() -> None:
    def make(tenant: str):
        def orders(raw: object, tenant: str = tenant) -> str:
            return tenant

        return orders

    project = compile_models(
        [
            ModelDef(name="orders_acme", fn=make("acme")),
            ModelDef(name="orders_globex", fn=make("globex")),
        ]
    )
    assert project.models["orders_acme"].fingerprint != project.models["orders_globex"].fingerprint
