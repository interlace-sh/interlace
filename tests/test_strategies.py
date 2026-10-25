"""Strategy AST builders and the resolver."""

from __future__ import annotations

import pytest

from interlace.engines.base import EngineCaps
from interlace.ir.relation import SqlRelation, TableRef
from interlace.strategies import FullMerge, HashMerge, Incremental, Merge, Replace, View, resolve_strategy
from interlace.strategies.base import WritePlan

pytestmark = pytest.mark.unit

_CAPS = EngineCaps(supports_create_or_replace=True)
_NO_REPLACE = EngineCaps(supports_create_or_replace=False)
_MERGE_CAPS = EngineCaps(supports_create_or_replace=True, supports_merge=True)
_TARGET = TableRef(schema="interlace__main", name="orders__abc")


def _relation() -> SqlRelation:
    return SqlRelation.from_sql("SELECT 1 AS x")


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


def test_sqlglot30_drop_ignores_this_and_drop_helper_names_the_table() -> None:
    """sqlglot 30 stores ``this=`` but the generator only emits ``tables=``."""
    from sqlglot import exp

    from interlace.ir.relation import drop

    silent = exp.Drop(this=_TARGET.to_expr(), kind="TABLE", exists=True).sql(dialect="duckdb")
    assert silent == "DROP TABLE IF EXISTS"
    assert drop(_TARGET, kind="TABLE").sql(dialect="duckdb") == "DROP TABLE IF EXISTS interlace__main.orders__abc"


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
    # cap on, but the caller has no column list (e.g. a first delivery, where the target
    # doesn't exist yet and there is nothing to preserve) -> column-agnostic path
    statements = Merge(("id",)).plan_statements(_relation(), _TARGET, _MERGE_CAPS)
    assert len(statements) == 3


def test_merge_without_native_merge_upserts_in_place() -> None:
    """No MERGE, but a column list: UPDATE the matched keys + INSERT the rest, both
    naming their columns — a column outside the list is never written."""
    statements = Merge(("id",)).plan_statements(_relation(), _TARGET, _CAPS, columns=["id", "x"])
    rendered = _sql(statements)
    assert len(rendered) == 2
    assert rendered[0].startswith("UPDATE interlace__main.orders__abc SET x = _s.x FROM (SELECT 1 AS x) AS _s")
    assert rendered[0].endswith("WHERE orders__abc.id = _s.id")
    assert rendered[1].startswith("INSERT INTO interlace__main.orders__abc (id, x) SELECT _s.id, _s.x FROM")
    # NOT EXISTS, not "key NOT IN (SELECT key FROM target)": one NULL key in the target
    # would make that subquery NULL for every row and insert nothing at all.
    assert "NOT EXISTS(SELECT 1 FROM interlace__main.orders__abc WHERE orders__abc.id = _s.id)" in rendered[1]


def test_merge_without_native_merge_multi_key() -> None:
    rendered = _sql(Merge(("a", "b")).plan_statements(_relation(), _TARGET, _CAPS, columns=["a", "b", "v"]))
    assert "SET v = _s.v" in rendered[0]  # only non-key columns
    assert "WHERE orders__abc.a = _s.a AND orders__abc.b = _s.b" in rendered[0]
    assert "WHERE orders__abc.a = _s.a AND orders__abc.b = _s.b" in rendered[1]  # correlated NOT EXISTS


def test_merge_without_native_merge_key_only_table_is_insert_only() -> None:
    statements = Merge(("id",)).plan_statements(_relation(), _TARGET, _CAPS, columns=["id"])
    assert len(statements) == 1  # nothing to update when every column is a key
    assert _sql(statements)[0].startswith("INSERT INTO interlace__main.orders__abc (id) SELECT _s.id FROM")


def test_merge_key_only_table_omits_the_update_clause() -> None:
    statements = Merge(("id",)).plan_statements(_relation(), _TARGET, _MERGE_CAPS, columns=["id"])
    sql = _sql(statements)[0]
    assert "WHEN MATCHED" not in sql  # nothing to update when every column is a key
    assert "WHEN NOT MATCHED THEN INSERT (id) VALUES (_s.id)" in sql


_DUCKLAKE = EngineCaps(supports_create_or_replace=True, except_in_delete=False)
_NO_SUBQUERY = EngineCaps(supports_mutation_subquery=False)


def _counts_for(statements: WritePlan, *, changed: int, vanished: int, inserted: int) -> list[int]:
    """Place affected-row counts by role, so a new staging statement does not shift the test."""
    placed = {"upsert_delete": changed, "delete": vanished, "insert": inserted}
    return [placed.get(role, 0) for role in statements.roles]


def test_full_merge_reads_the_source_once_and_inlines_except_in_the_delete() -> None:
    relation = SqlRelation.from_sql("SELECT id, v FROM src")
    statements = FullMerge(("id",)).plan_statements(relation, _TARGET, _CAPS)
    rendered = _sql(statements)
    # The model's query is snapshotted once. Both EXCEPT passes read that temp table,
    # so a non-deterministic source cannot delete one version of a key and insert another.
    assert [sql for sql in rendered if "FROM src" in sql] == [
        sql for sql in rendered if sql.startswith("CREATE TEMPORARY TABLE _interlace_fm_src_")
    ]
    assert len([sql for sql in rendered if "FROM src" in sql]) == 1
    other = _sql(FullMerge(("id",)).plan_statements(relation, _TARGET, _CAPS))
    assert rendered[0] != other[0]  # parallel merges must not share a connection-scoped temp name
    assert rendered[0].startswith("DROP TABLE IF EXISTS _interlace_fm_src_")
    assert rendered[-1] == rendered[0]
    ensure = next(sql for sql in rendered if sql.startswith("CREATE TABLE IF NOT EXISTS"))
    assert "_interlace_fm_src_" in ensure and "FROM src" not in ensure
    deletes = [sql for sql in rendered if sql.startswith("DELETE")]
    assert deletes[0].startswith("DELETE FROM interlace__main.orders__abc WHERE id IN")
    assert "EXCEPT" in deletes[0] and "_interlace_fm_src_" in deletes[0] and "FROM src" not in deletes[0]
    assert deletes[1].startswith("DELETE") and "EXCEPT" not in deletes[1]
    insert = next(sql for sql in rendered if sql.startswith("INSERT"))
    assert insert.startswith("INSERT INTO interlace__main.orders__abc ") and "EXCEPT" in insert
    # delete changed=11, delete vanished=2, insert=20 -> +9 ~11 -2
    counts = statements.row_counts(_counts_for(statements, changed=11, vanished=2, inserted=20))
    assert (counts.inserted, counts.updated, counts.deleted) == (9, 11, 2)
    postgres_deletes = [
        statement.sql(dialect="postgres")
        for statement in statements
        if statement.sql(dialect="duckdb").startswith("DELETE")
    ]
    assert any("EXCEPT" in sql for sql in postgres_deletes)


def test_full_merge_stages_key_sets_when_the_delete_cannot_hold_except() -> None:
    """DuckLake aborts if EXCEPT is nested inside the DELETE. The deletes read temp tables."""
    relation = SqlRelation.from_sql("SELECT id, v FROM src")
    statements = FullMerge(("id",)).plan_statements(relation, _TARGET, _DUCKLAKE)
    rendered = _sql(statements)
    # One evaluation of the model. The key sets and the insert read the snapshot.
    assert [sql for sql in rendered if "FROM src" in sql] == [
        sql for sql in rendered if sql.startswith("CREATE TEMPORARY TABLE _interlace_fm_src_")
    ]
    assert rendered[0].startswith("DROP TABLE IF EXISTS _interlace_fm_src_")
    assert "DROP TABLE IF EXISTS _interlace_fm_changed" in rendered
    changed_stage = next(sql for sql in rendered if sql.startswith("CREATE TEMPORARY TABLE _interlace_fm_changed AS"))
    assert "EXCEPT" in changed_stage and "_interlace_fm_src_" in changed_stage and "FROM src" not in changed_stage
    vanished_stage = next(sql for sql in rendered if sql.startswith("CREATE TEMPORARY TABLE _interlace_fm_vanished AS"))
    assert "WHERE NOT id IN" in vanished_stage and "EXCEPT" not in vanished_stage
    changed = "DELETE FROM interlace__main.orders__abc WHERE id IN (SELECT id FROM _interlace_fm_changed)"
    vanished = "DELETE FROM interlace__main.orders__abc WHERE id IN (SELECT id FROM _interlace_fm_vanished)"
    assert changed in rendered and vanished in rendered
    insert = next(sql for sql in rendered if sql.startswith("INSERT"))
    assert "EXCEPT" in insert and "_interlace_fm_src_" in insert
    assert rendered[-1] == rendered[0]

    counts = statements.row_counts(_counts_for(statements, changed=11, vanished=2, inserted=20))
    assert (counts.inserted, counts.updated, counts.deleted) == (9, 11, 2)

    multi = _sql(FullMerge(("a", "b")).plan_statements(relation, _TARGET, _DUCKLAKE))
    assert any("WHERE (a, b) IN (SELECT a, b FROM _interlace_fm_changed)" in sql for sql in multi)
    assert any("WHERE (a, b) IN (SELECT a, b FROM _interlace_fm_vanished)" in sql for sql in multi)
    postgres = [statement.sql(dialect="postgres") for statement in statements]
    assert any(sql.startswith("CREATE TEMPORARY TABLE _interlace_fm_changed AS") for sql in postgres)
    assert all("EXCEPT" not in sql for sql in postgres if sql.startswith("DELETE"))


def test_full_merge_rejects_engines_without_delete_subqueries() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError, match="subquery"):
        FullMerge(("id",)).plan_statements(SqlRelation.from_sql("SELECT id FROM src"), _TARGET, _NO_SUBQUERY)


def test_merge_row_counts_follow_the_plan_shape() -> None:
    # native MERGE returns one combined affected-row count (no insert/update split)
    native = Merge(("id",)).plan_statements(_relation(), _TARGET, _MERGE_CAPS, columns=["id", "x"])
    assert native.row_counts([7]).inserted == 7
    # the in-place fallback splits directly: [update=2, insert=5]
    in_place = Merge(("id",)).plan_statements(_relation(), _TARGET, _CAPS, columns=["id", "x"])
    counted = in_place.row_counts([2, 5])
    assert (counted.inserted, counted.updated) == (5, 2)
    # the DELETE+INSERT fallback keeps the split: [ensure, delete=2, insert=5] -> +3 ~2
    delete_insert = Merge(("id",)).plan_statements(_relation(), _TARGET, _CAPS)
    counts = delete_insert.row_counts([0, 2, 5])
    assert (counts.inserted, counts.updated) == (3, 2)


def test_hash_merge_builds_ensure_update_insert() -> None:
    statements = HashMerge(("id",)).plan_statements(SqlRelation.from_sql("SELECT id, v FROM src"), _TARGET, _CAPS)
    rendered = _sql(statements)
    assert rendered[0].startswith("CREATE TABLE IF NOT EXISTS interlace__main.orders__abc AS")
    # MD5 over the non-key columns, each NULL-normalised and joined on a separator.
    # Asserted by its parts, not as one string: sqlglot 30 wraps CONCAT_WS in a NULL
    # guard which is inert here (literal separator, every argument COALESCE'd) but
    # changes the rendering. tests/test_example_materialisations.py covers the effect.
    assert "MD5(" in rendered[0]
    assert "CONCAT_WS('||', COALESCE(CAST(v AS TEXT), ''))" in rendered[0]
    assert "AS _hash" in rendered[0]
    assert rendered[1].startswith("UPDATE interlace__main.orders__abc SET v = _s.v, _hash = _s._hash FROM")
    assert "orders__abc._hash <> _s._hash AND orders__abc.id = _s.id" in rendered[1]  # changed keys only
    assert rendered[2].startswith("INSERT INTO interlace__main.orders__abc SELECT * FROM")
    assert "NOT EXISTS(SELECT 1 FROM interlace__main.orders__abc WHERE orders__abc.id = _s.id)" in rendered[2]


def test_hash_merge_names_its_insert_columns_when_aligned() -> None:
    """With a column list (an existing target, which may carry columns this model does
    not produce) the insert binds by name and leaves the rest to their DEFAULTs."""
    statements = HashMerge(("id",)).plan_statements(
        SqlRelation.from_sql("SELECT id, v FROM src"), _TARGET, _CAPS, columns=["id", "v"]
    )
    assert _sql(statements)[2].startswith("INSERT INTO interlace__main.orders__abc (id, v, _hash) SELECT * FROM")


def test_append_names_its_columns_when_aligned() -> None:
    from interlace.strategies import Append

    positional = _sql(Append().plan_statements(_relation(), _TARGET, _CAPS))
    assert positional[1] == "INSERT INTO interlace__main.orders__abc SELECT 1 AS x"
    named = _sql(Append().plan_statements(_relation(), _TARGET, _CAPS, columns=["x"]))
    assert named[1] == "INSERT INTO interlace__main.orders__abc (x) SELECT 1 AS x"


def test_hash_merge_row_counts_split_update_and_insert() -> None:
    planned = HashMerge(("id",)).plan_statements(SqlRelation.from_sql("SELECT id, v FROM src"), _TARGET, _CAPS)
    counts = planned.row_counts([0, 3, 5])  # [ensure, update=3 changed, insert=5 new]
    assert (counts.inserted, counts.updated) == (5, 3)


def test_hash_merge_needs_explicit_columns_for_select_star() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):  # the hash is built from the projection — SELECT * can't be enumerated
        HashMerge(("id",)).plan_statements(SqlRelation.from_sql("SELECT * FROM src"), _TARGET, _CAPS)


def test_resolve_strategy_hash_merge() -> None:
    assert isinstance(resolve_strategy("virtual", "hash_merge", ("id",)), HashMerge)


def test_incremental_builds_windowed_statements() -> None:
    from datetime import datetime

    from interlace.state.interval import Interval

    window = Interval(datetime(2026, 1, 1), datetime(2026, 1, 2))
    rendered = _sql(Incremental("ts").plan_statements(_relation(), _TARGET, _CAPS, window))
    assert rendered[0].startswith("CREATE TABLE IF NOT EXISTS")
    assert rendered[1] == (
        "DELETE FROM interlace__main.orders__abc WHERE ts >= '2026-01-01T00:00:00' AND ts < '2026-01-02T00:00:00'"
    )
    assert "WHERE ts >= '2026-01-01T00:00:00' AND ts < '2026-01-02T00:00:00'" in rendered[2]


def test_incremental_requires_an_interval() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        Incremental("ts").plan_statements(_relation(), _TARGET, _CAPS, None)


def test_resolve_strategy_picks_implementations() -> None:
    assert isinstance(resolve_strategy("virtual", "replace"), Replace)
    assert isinstance(resolve_strategy("view", "replace"), View)
    assert isinstance(resolve_strategy("virtual", "merge", ("id",)), Merge)
    assert isinstance(resolve_strategy("virtual", "incremental", time_column="ts"), Incremental)


def test_resolve_strategy_incremental_requires_time_column() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        resolve_strategy("virtual", "incremental")


def test_resolve_strategy_merge_requires_key() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        resolve_strategy("virtual", "merge")  # no key


def test_resolve_strategy_rejects_unsupported() -> None:
    from interlace.exceptions import PlanError

    with pytest.raises(PlanError):
        resolve_strategy("virtual", "scd2")
