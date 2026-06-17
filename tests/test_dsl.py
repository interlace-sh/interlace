"""Decorator declarations register into the project registry."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from interlace.dsl.decorators import REGISTRY, check, model, stream
from interlace.exceptions import DefinitionError

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _clean_registry() -> Iterator[None]:
    REGISTRY.clear()
    yield
    REGISTRY.clear()


def test_model_registers_with_function_name_by_default() -> None:
    @model()
    def orders() -> None: ...

    assert "orders" in REGISTRY.models
    assert REGISTRY.models["orders"].materialise == "table"


def test_model_explicit_name_and_key_normalisation() -> None:
    @model(name="silver.orders", strategy="merge_by_key", key="order_id")
    def _build() -> None: ...

    definition = REGISTRY.models["silver.orders"]
    assert definition.key == ("order_id",)
    assert definition.strategy == "merge_by_key"


def test_duplicate_model_raises() -> None:
    @model(name="dup")
    def _a() -> None: ...

    with pytest.raises(DefinitionError):

        @model(name="dup")
        def _b() -> None: ...


def test_invalid_materialise_raises() -> None:
    with pytest.raises(DefinitionError):

        @model(materialise="nonsense")
        def _bad() -> None: ...


def test_stream_and_check_register() -> None:
    @stream("orders_raw", schema={"order_id": "string"}, idempotency_key="order_id")
    def _orders_raw() -> None: ...

    @check(model="orders_raw")
    def not_empty() -> bool:
        return True

    assert "orders_raw" in REGISTRY.streams
    assert REGISTRY.streams["orders_raw"].idempotency_key == "order_id"
    assert REGISTRY.checks[0].model == "orders_raw"
