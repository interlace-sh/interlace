"""Strategy AST builders and the resolver."""

from __future__ import annotations

import pytest
import sqlglot

from interlace.engines.base import EngineCaps
from interlace.ir.relation import SqlRelation, TableRef
from interlace.strategies import FullRefresh, IncrementalByTime, MergeByKey, View, resolve_strategy

pytestmark = pytest.mark.unit

_CAPS = EngineCaps(supports_create_or_replace=True)
_NO_REPLACE = EngineCaps(supports_create_or_replace=False)
_TARGET = TableRef(schema="interlace__main", name="orders__abc")


def _relation() -> SqlRelation:
    return SqlRelation(ast=sqlglot.parse_one("SELECT 1 AS x"))


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


def test_merge_by_key_builds_create_delete_insert() -> None:
    statements = MergeByKey(("id",)).plan_statements(_relation(), _TARGET, _CAPS)
    rendered = _sql(statements)
    assert rendered[0].startswith("CREATE TABLE IF NOT EXISTS interlace__main.orders__abc AS")
    assert rendered[1] == "DELETE FROM interlace__main.orders__abc WHERE id IN (SELECT id FROM (SELECT 1 AS x) AS _s)"
    assert rendered[2] == "INSERT INTO interlace__main.orders__abc SELECT 1 AS x"


def test_merge_by_key_multi_key_predicate() -> None:
    statements = MergeByKey(("a", "b")).plan_statements(_relation(), _TARGET, _CAPS)
    assert "(a, b) IN (SELECT a, b FROM" in _sql(statements)[1]


def test_incremental_by_time_builds_windowed_statements() -> None:
    from datetime import datetime

    from interlace.state.interval import Interval

    window = Interval(datetime(2026, 1, 1), datetime(2026, 1, 2))
    rendered = _sql(IncrementalByTime("ts").plan_statements(_relation(), _TARGET, _CAPS, window))
    assert rendered[0].startswith("CREATE TABLE IF NOT EXISTS")
    assert rendered[1] == (
        "DELETE FROM interlace__main.orders__abc WHERE ts >= '2026-01-01T00:00:00' AND ts < '2026-01-02T00:00:00'"
    )
    assert "WHERE ts >= '2026-01-01T00:00:00' AND ts < '2026-01-02T00:00:00'" in rendered[2]


def test_incremental_by_time_requires_an_interval() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        IncrementalByTime("ts").plan_statements(_relation(), _TARGET, _CAPS, None)


def test_resolve_strategy_picks_implementations() -> None:
    assert isinstance(resolve_strategy("table", "full"), FullRefresh)
    assert isinstance(resolve_strategy("view", "full"), View)
    assert isinstance(resolve_strategy("table", "merge_by_key", ("id",)), MergeByKey)
    assert isinstance(resolve_strategy("table", "incremental_by_time", time_column="ts"), IncrementalByTime)


def test_resolve_strategy_incremental_requires_time_column() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        resolve_strategy("table", "incremental_by_time")


def test_resolve_strategy_merge_requires_key() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        resolve_strategy("table", "merge_by_key")  # no key


def test_resolve_strategy_rejects_unsupported() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        resolve_strategy("table", "scd2")
