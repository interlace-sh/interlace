"""
Tests for strategy implementations.

Tests cover:
- MergeByKeyStrategy (single and multi-column keys, NULL key handling)
- SCDType2Strategy (empty tracked_columns guard, NULL key handling)
- AppendStrategy
- ReplaceStrategy
- NoneStrategy
"""

import ibis
import pytest

from interlace.strategies.append import AppendStrategy
from interlace.strategies.merge_by_key import MergeByKeyStrategy
from interlace.strategies.none import NoneStrategy
from interlace.strategies.replace import ReplaceStrategy
from interlace.strategies.scd_type_2 import SCDType2Strategy


class TestMergeByKeyStrategy:
    """Test MergeByKeyStrategy implementation."""

    def test_generate_sql_single_key(self):
        """Test SQL generation for single column key."""
        strategy = MergeByKeyStrategy()
        con = ibis.duckdb.connect()

        # Create temp table to get schema
        temp_data = ibis.memtable([{"id": 1, "name": "Test"}])
        df = temp_data.execute()
        con.create_table("_temp_source", obj=df, temp=True)

        sql = strategy.generate_sql(con, "target_table", "public", "_temp_source", "id")

        assert "MERGE INTO" in sql
        assert "IS NOT DISTINCT FROM" in sql
        assert "WHEN MATCHED THEN UPDATE" in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql

    def test_generate_sql_multi_key(self):
        """Test SQL generation for multi-column key."""
        strategy = MergeByKeyStrategy()
        con = ibis.duckdb.connect()

        # Create temp table to get schema
        temp_data = ibis.memtable([{"user_id": 1, "order_id": 1, "amount": 100}])
        df = temp_data.execute()
        con.create_table("_temp_source", obj=df, temp=True)

        sql = strategy.generate_sql(con, "target_table", "public", "_temp_source", ["user_id", "order_id"])

        assert "MERGE INTO" in sql
        assert "IS NOT DISTINCT FROM" in sql
        assert "AND" in sql  # Multi-column condition

    def test_merge_null_keys_no_duplicates(self):
        """Test that NULL primary keys don't produce duplicate inserts via IS NOT DISTINCT FROM."""
        strategy = MergeByKeyStrategy()
        con = ibis.duckdb.connect()

        # Create target table with a NULL key row (DuckDB default schema is 'main')
        con.raw_sql("CREATE TABLE main.merge_target (id INTEGER, name VARCHAR)")
        con.raw_sql("INSERT INTO main.merge_target VALUES (NULL, 'original')")

        # Create source with typed schema (ibis can't infer type from all-NULL column)
        con.raw_sql("CREATE TEMP TABLE _merge_source (id INTEGER, name VARCHAR)")
        con.raw_sql("INSERT INTO _merge_source VALUES (NULL, 'updated')")

        sql = strategy.generate_sql(con, "merge_target", "main", "_merge_source", "id")

        # Execute the merge
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.raw_sql(stmt)

        # Verify: should have exactly 1 row (updated), not 2 (duplicate)
        result = con.raw_sql("SELECT COUNT(*) as cnt FROM main.merge_target").fetchone()
        assert result[0] == 1

        # Verify the value was updated
        result = con.raw_sql("SELECT name FROM main.merge_target WHERE id IS NULL").fetchone()
        assert result[0] == "updated"


class TestSCDType2Strategy:
    """Test SCDType2Strategy implementation."""

    def test_all_columns_are_pks_raises_error(self):
        """SCD2 should raise ValueError when all source columns are primary keys."""
        strategy = SCDType2Strategy()
        con = ibis.duckdb.connect()

        # Create source where all columns are PKs
        source_data = ibis.memtable([{"id": 1, "code": "A"}])
        df = source_data.execute()
        con.create_table("_scd2_source", obj=df, temp=True)

        # Create target table with SCD2 metadata columns
        con.raw_sql(
            "CREATE TABLE main.scd2_target ("
            "id INTEGER, code VARCHAR, "
            "valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN, _scd2_hash VARCHAR)"
        )

        with pytest.raises(ValueError, match="at least one non-primary-key column"):
            strategy.generate_sql(
                con,
                "scd2_target",
                "main",
                "_scd2_source",
                primary_key=["id", "code"],
            )

    def test_all_columns_are_pks_raises_on_initial_insert(self):
        """SCD2 initial insert should also raise ValueError when all columns are PKs."""
        strategy = SCDType2Strategy()
        con = ibis.duckdb.connect()

        source_data = ibis.memtable([{"id": 1}])
        df = source_data.execute()
        con.create_table("_scd2_init_src", obj=df, temp=True)

        con.raw_sql(
            "CREATE TABLE main.scd2_init_target ("
            "id INTEGER, "
            "valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN, _scd2_hash VARCHAR)"
        )

        with pytest.raises(ValueError, match="at least one non-primary-key column"):
            strategy.get_initial_insert_sql(
                con,
                "scd2_init_target",
                "main",
                "_scd2_init_src",
                primary_key="id",
            )

    def test_generate_sql_uses_is_not_distinct_from(self):
        """SCD2 ON condition should use IS NOT DISTINCT FROM for NULL-safe matching."""
        strategy = SCDType2Strategy()
        con = ibis.duckdb.connect()

        source_data = ibis.memtable([{"id": 1, "name": "Test"}])
        df = source_data.execute()
        con.create_table("_scd2_ndistinct", obj=df, temp=True)

        con.raw_sql(
            "CREATE TABLE main.scd2_nd_target ("
            "id INTEGER, name VARCHAR, "
            "valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN, _scd2_hash VARCHAR)"
        )

        sql = strategy.generate_sql(con, "scd2_nd_target", "main", "_scd2_ndistinct", primary_key="id")

        assert "IS NOT DISTINCT FROM" in sql


class TestAppendStrategy:
    """Test AppendStrategy implementation."""

    def test_generate_sql(self):
        """Test SQL generation for append strategy."""
        strategy = AppendStrategy()
        con = ibis.duckdb.connect()

        # Create temp table to get schema
        temp_data = ibis.memtable([{"id": 1, "name": "Test"}])
        df = temp_data.execute()
        con.create_table("_temp_source", obj=df, temp=True)

        sql = strategy.generate_sql(con, "target_table", "public", "_temp_source")

        assert "INSERT INTO" in sql
        assert "SELECT" in sql


class TestReplaceStrategy:
    """Test ReplaceStrategy implementation."""

    def test_replace_strategy(self):
        """Test replace strategy (no SQL generation, handled by executor)."""
        strategy = ReplaceStrategy()

        # Replace strategy doesn't generate SQL - executor handles it directly
        # via _apply_replace_strategy method
        sql = strategy.generate_sql(None, "target_table", "public", "source_table")
        assert sql is None

        # Replace strategy doesn't need temp table
        assert not strategy.needs_temp_table()


class TestNoneStrategy:
    """Test NoneStrategy implementation."""

    def test_none_strategy(self):
        """Test none strategy (no SQL generation)."""
        strategy = NoneStrategy()
        con = ibis.duckdb.connect()

        # None strategy doesn't generate SQL
        sql = strategy.generate_sql(con, "target_table", "public", "source_table")
        assert sql is None

        # None strategy doesn't need temp table
        assert not strategy.needs_temp_table()
