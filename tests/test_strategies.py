"""
Tests for strategy implementations.

Tests cover:
- MergeByKeyStrategy (single and multi-column keys)
- AppendStrategy
- ReplaceStrategy
- NoneStrategy
"""

import pytest
import ibis
from interlace.strategies.merge_by_key import MergeByKeyStrategy
from interlace.strategies.append import AppendStrategy
from interlace.strategies.replace import ReplaceStrategy
from interlace.strategies.none import NoneStrategy


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
        
        sql = strategy.generate_sql(
            con, "target_table", "public", "_temp_source", "id"
        )
        
        assert "MERGE INTO" in sql
        assert 'target."id" = source."id"' in sql or "target.id = source.id" in sql
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
        
        sql = strategy.generate_sql(
            con, "target_table", "public", "_temp_source", ["user_id", "order_id"]
        )
        
        assert "MERGE INTO" in sql
        assert ('target."user_id" = source."user_id"' in sql or "target.user_id = source.user_id" in sql)
        assert ('target."order_id" = source."order_id"' in sql or "target.order_id = source.order_id" in sql)
        assert "AND" in sql  # Multi-column condition


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
        assert strategy.needs_temp_table() == False


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
        assert strategy.needs_temp_table() == False

