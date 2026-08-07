"""Strategy AST builders and the resolver."""

from __future__ import annotations

import pytest
import sqlglot

from interlace.engines.base import EngineCaps
from interlace.ir.relation import SqlRelation, TableRef
from interlace.strategies import HashMerge, IncrementalByTime, Merge, Replace, View, resolve_strategy

pytestmark = pytest.mark.unit

_CAPS = EngineCaps(supports_create_or_replace=True)
_NO_REPLACE = EngineCaps(supports_create_or_replace=False)
_MERGE_CAPS = EngineCaps(supports_create_or_replace=True, supports_merge=True)
_TARGET = TableRef(schema="interlace__main", name="orders__abc")


def _relation() -> SqlRelation:
    return SqlRelation(ast=sqlglot.parse_one("SELECT 1 AS x"))


def _sql(statements: list) -> list[str]:
    return [s.sql(dialect="duckdb") for s in statements]


def test_replace_uses_create_or_replace_when_supported() -> None:
    statements = Replace().plan_statements(_relation(), _TARGET, _CAPS)
    assert _sql(statements) == ["CREATE OR REPLACE TABLE interlace__main.orders__abc AS SELECT 1 AS x"]


def test_replace_falls_back_to_drop_and_create() -> None:
    statements = Replace().plan_statements(_relation(), _TARGET, _NO_REPLACE)
    assert _sql(statements) == [
        "DROP TABLE IF EXISTS interlace__main.orders__abc",
        "CREATE TABLE interlace__main.orders__abc AS SELECT 1 AS x",
    ]


def test_view_strategy_creates_a_view() -> None:
    statements = View().plan_statements(_relation(), _TARGET, _CAPS)
    assert _sql(statements) == ["CREATE OR REPLACE VIEW interlace__main.orders__abc AS SELECT 1 AS x"]


def test_merge_builds_create_delete_insert() -> None:
    statements = Merge(("id",)).plan_statements(_relation(), _TARGET, _CAPS)
    rendered = _sql(statements)
    assert rendered[0].startswith("CREATE TABLE IF NOT EXISTS interlace__main.orders__abc AS")
    assert rendered[1] == "DELETE FROM interlace__main.orders__abc WHERE id IN (SELECT id FROM (SELECT 1 AS x) AS _s)"
    assert rendered[2] == "INSERT INTO interlace__main.orders__abc SELECT 1 AS x"


def test_merge_multi_key_predicate() -> None:
    statements = Merge(("a", "b")).plan_statements(_relation(), _TARGET, _CAPS)
    assert "(a, b) IN (SELECT a, b FROM" in _sql(statements)[1]


def test_merge_uses_native_merge_when_supported_and_columns_known() -> None:
    statements = Merge(("id",)).plan_statements(_relation(), _TARGET, _MERGE_CAPS, columns=["id", "x"])
    assert len(statements) == 1
    sql = _sql(statements)[0]
    assert sql.startswith("MERGE INTO interlace__main.orders__abc AS _t USING (SELECT 1 AS x) AS _s")
    assert "ON _t.id = _s.id" in sql
    assert "WHEN MATCHED THEN UPDATE SET x = _s.x" in sql
    assert "WHEN NOT MATCHED THEN INSERT (id, x) VALUES (_s.id, _s.x)" in sql


def test_merge_native_multi_key_on_predicate_and_non_key_set() -> None:
    statements = Merge(("a", "b")).plan_statements(_relation(), _TARGET, _MERGE_CAPS, columns=["a", "b", "v"])
    sql = _sql(statements)[0]
    assert "ON _t.a = _s.a AND _t.b = _s.b" in sql
    assert "WHEN MATCHED THEN UPDATE SET v = _s.v" in sql  # only non-key columns in the SET


def test_merge_falls_back_to_delete_insert_without_columns() -> None:
    # cap on, but the caller has no column list (e.g. a first delivery) -> portable path
    statements = Merge(("id",)).plan_statements(_relation(), _TARGET, _MERGE_CAPS)
    assert len(statements) == 3


def test_merge_key_only_table_omits_the_update_clause() -> None:
    statements = Merge(("id",)).plan_statements(_relation(), _TARGET, _MERGE_CAPS, columns=["id"])
    sql = _sql(statements)[0]
    assert "WHEN MATCHED" not in sql  # nothing to update when every column is a key
    assert "WHEN NOT MATCHED THEN INSERT (id) VALUES (_s.id)" in sql


def test_merge_row_counts_native_is_a_single_written_count() -> None:
    # native MERGE returns one combined affected-row count (no insert/update split)
    assert Merge(("id",)).row_counts([7]).inserted == 7
    # the DELETE+INSERT fallback keeps the split: [ensure, delete=2, insert=5] -> +3 ~2
    counts = Merge(("id",)).row_counts([0, 2, 5])
    assert (counts.inserted, counts.updated) == (3, 2)


def test_hash_merge_builds_ensure_update_insert() -> None:
    statements = HashMerge(("id",)).plan_statements(
        SqlRelation(ast=sqlglot.parse_one("SELECT id, v FROM src")), _TARGET, _CAPS
    )
    rendered = _sql(statements)
    assert rendered[0].startswith("CREATE TABLE IF NOT EXISTS interlace__main.orders__abc AS")
    assert "MD5(CONCAT_WS('||', COALESCE(CAST(v AS TEXT), ''))) AS _hash" in rendered[0]  # hash over the non-key col
    assert rendered[1].startswith("UPDATE interlace__main.orders__abc SET v = _s.v, _hash = _s._hash FROM")
    assert "orders__abc._hash <> _s._hash AND orders__abc.id = _s.id" in rendered[1]  # changed keys only
    assert rendered[2].startswith("INSERT INTO interlace__main.orders__abc SELECT * FROM")
    assert "NOT _s.id IN (SELECT id FROM interlace__main.orders__abc)" in rendered[2]  # new keys only


def test_hash_merge_row_counts_split_update_and_insert() -> None:
    counts = HashMerge(("id",)).row_counts([0, 3, 5])  # [ensure, update=3 changed, insert=5 new]
    assert (counts.inserted, counts.updated) == (5, 3)


def test_hash_merge_needs_explicit_columns_for_select_star() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):  # the hash is built from the projection — SELECT * can't be enumerated
        HashMerge(("id",)).plan_statements(SqlRelation(ast=sqlglot.parse_one("SELECT * FROM src")), _TARGET, _CAPS)


def test_resolve_strategy_hash_merge() -> None:
    assert isinstance(resolve_strategy("virtual", "hash_merge", ("id",)), HashMerge)


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
    assert isinstance(resolve_strategy("virtual", "replace"), Replace)
    assert isinstance(resolve_strategy("view", "replace"), View)
    assert isinstance(resolve_strategy("virtual", "merge", ("id",)), Merge)
    assert isinstance(resolve_strategy("virtual", "incremental_by_time", time_column="ts"), IncrementalByTime)


def test_resolve_strategy_incremental_requires_time_column() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        resolve_strategy("virtual", "incremental_by_time")


def test_resolve_strategy_merge_requires_key() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        resolve_strategy("virtual", "merge")  # no key


def test_resolve_strategy_rejects_unsupported() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        resolve_strategy("virtual", "scd2")
