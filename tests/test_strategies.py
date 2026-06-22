"""Strategy AST builders and the resolver."""

from __future__ import annotations

import pytest
import sqlglot

from interlace.engines.base import EngineCaps
from interlace.ir.relation import EngineRef, SqlRelation, TableRef
from interlace.ir.schema import empty_schema
from interlace.strategies import FullRefresh, View, resolve_strategy

pytestmark = pytest.mark.unit

_CAPS = EngineCaps(supports_create_or_replace=True)
_NO_REPLACE = EngineCaps(supports_create_or_replace=False)
_TARGET = TableRef(schema="interlace__main", name="orders__abc")


def _relation() -> SqlRelation:
    return SqlRelation(
        ast=sqlglot.parse_one("SELECT 1 AS x"), engine=EngineRef("duckdb", "duckdb"), schema=empty_schema()
    )


def _sql(statements: list) -> list[str]:
    return [s.sql(dialect="duckdb") for s in statements]


def test_full_refresh_uses_create_or_replace_when_supported() -> None:
    statements = FullRefresh().plan_statements(_relation(), _TARGET, _CAPS)
    assert _sql(statements) == ["CREATE OR REPLACE TABLE interlace__main.orders__abc AS SELECT 1 AS x"]


def test_full_refresh_falls_back_to_drop_and_create() -> None:
    statements = FullRefresh().plan_statements(_relation(), _TARGET, _NO_REPLACE)
    assert _sql(statements) == [
        "DROP TABLE IF EXISTS interlace__main.orders__abc",
        "CREATE TABLE interlace__main.orders__abc AS SELECT 1 AS x",
    ]


def test_view_strategy_creates_a_view() -> None:
    statements = View().plan_statements(_relation(), _TARGET, _CAPS)
    assert _sql(statements) == ["CREATE OR REPLACE VIEW interlace__main.orders__abc AS SELECT 1 AS x"]


def test_resolve_strategy_picks_implementations() -> None:
    assert isinstance(resolve_strategy("table", "full"), FullRefresh)
    assert isinstance(resolve_strategy("view", "full"), View)


def test_resolve_strategy_rejects_unsupported() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        resolve_strategy("table", "merge_by_key")
