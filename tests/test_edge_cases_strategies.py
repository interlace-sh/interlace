"""
Tests for edge cases and bugs in strategies and materialization.

Covers:
- Empty primary key validation
- Key-only table handling in merge strategy
- Temp table creation for SCD Type 2
"""

from unittest.mock import Mock, patch

import ibis
import pytest

# ---------------------------------------------------------------------------
# Bug 1 & 2: Empty primary_key list bypasses validation
# ---------------------------------------------------------------------------


class TestEmptyPrimaryKeyValidation:
    """Empty primary_key=[] should raise ValueError like primary_key=None."""

    @pytest.mark.unit
    def test_merge_by_key_empty_list_primary_key(self):
        """MergeByKeyStrategy should reject primary_key=[] with ValueError."""
        from interlace.strategies.merge_by_key import MergeByKeyStrategy

        strategy = MergeByKeyStrategy()

        mock_connection = Mock()
        mock_table = Mock()
        mock_schema = {
            "id": ibis.dtype("int64"),
            "name": ibis.dtype("string"),
        }
        mock_table.schema.return_value = mock_schema
        mock_connection.table.return_value = mock_table

        with pytest.raises(ValueError, match="primary_key"):
            strategy.generate_sql(
                connection=mock_connection,
                target_table="test_table",
                schema="public",
                source_table="_tmp_test",
                primary_key=[],
            )

    @pytest.mark.unit
    def test_scd_type_2_empty_list_primary_key(self):
        """SCDType2Strategy should reject primary_key=[] with ValueError."""
        from interlace.strategies.scd_type_2 import SCDType2Strategy

        strategy = SCDType2Strategy()

        mock_connection = Mock()
        mock_table = Mock()
        mock_schema = {
            "id": ibis.dtype("int64"),
            "name": ibis.dtype("string"),
        }
        mock_table.schema.return_value = mock_schema
        mock_connection.table.return_value = mock_table

        with pytest.raises(ValueError, match="primary_key"):
            strategy.generate_sql(
                connection=mock_connection,
                target_table="dim_test",
                schema="public",
                source_table="_tmp_test",
                primary_key=[],
            )

    @pytest.mark.unit
    def test_scd_type_2_initial_insert_empty_primary_key(self):
        """SCDType2Strategy.get_initial_insert_sql should reject primary_key=[]."""
        from interlace.strategies.scd_type_2 import SCDType2Strategy

        strategy = SCDType2Strategy()

        mock_connection = Mock()
        mock_table = Mock()
        mock_schema = {
            "id": ibis.dtype("int64"),
            "name": ibis.dtype("string"),
        }
        mock_table.schema.return_value = mock_schema
        mock_connection.table.return_value = mock_table

        with pytest.raises(ValueError, match="primary_key"):
            strategy.get_initial_insert_sql(
                connection=mock_connection,
                target_table="dim_test",
                schema="public",
                source_table="_tmp_test",
                primary_key=[],
            )


# ---------------------------------------------------------------------------
# Bug 3: Key-only table produces invalid UPDATE SET * in merge
# ---------------------------------------------------------------------------


class TestMergeKeyOnlyTable:
    """When all columns are primary keys, UPDATE SET * is non-portable."""

    @pytest.mark.unit
    def test_merge_by_key_all_columns_are_keys(self):
        """MergeByKeyStrategy should produce valid SQL when all columns are keys.

        With all columns as primary keys, there are no non-key columns to update.
        The generated SQL should NOT contain 'UPDATE SET *' (non-portable).
        Instead it should either skip the WHEN MATCHED clause or use a valid no-op.
        """
        from interlace.strategies.merge_by_key import MergeByKeyStrategy

        strategy = MergeByKeyStrategy()

        mock_connection = Mock()
        mock_table = Mock()
        # All columns are primary keys
        mock_schema = {
            "user_id": ibis.dtype("int64"),
            "group_id": ibis.dtype("int64"),
        }
        mock_table.schema.return_value = mock_schema
        mock_connection.table.return_value = mock_table

        sql = strategy.generate_sql(
            connection=mock_connection,
            target_table="user_groups",
            schema="public",
            source_table="_tmp_user_groups",
            primary_key=["user_id", "group_id"],
        )

        # The SQL should be valid - no 'UPDATE SET *'
        assert "UPDATE SET *" not in sql
        # Should still have INSERT for new rows
        assert "INSERT" in sql
        assert "MERGE" in sql


# ---------------------------------------------------------------------------
# Bug 4: prepare_reference_table doesn't handle scd_type_2
# ---------------------------------------------------------------------------


class TestPrepareReferenceTableSCD2:
    """prepare_reference_table should create temp table for scd_type_2 strategy."""

    @pytest.mark.unit
    def test_prepare_reference_table_creates_temp_for_scd2(self):
        """SchemaManager.prepare_reference_table should create temp table for scd_type_2.

        The method only creates temp tables for 'merge_by_key' strategy,
        but scd_type_2 also requires a temp table (needs_temp_table() returns True).
        Without the temp table, apply_scd_type_2_strategy fails when trying to
        access connection.table(ref_table_name).
        """
        from interlace.core.execution.data_converter import DataConverter
        from interlace.core.execution.schema_manager import SchemaManager

        # Create minimal SchemaManager
        mock_data_converter = Mock(spec=DataConverter)
        schema_manager = SchemaManager(
            data_converter=mock_data_converter,
            schema_cache={},
            table_existence_cache={},
        )

        # Mock connection
        mock_connection = Mock()
        mock_connection.create_table = Mock()
        mock_table = Mock()
        mock_connection.table.return_value = mock_table

        # Mock check_table_exists to return False (temp table doesn't exist yet)
        with patch("interlace.core.execution.schema_manager.check_table_exists", return_value=False):
            mock_new_data = Mock()

            source_table, is_ref = schema_manager.prepare_reference_table(
                new_data=mock_new_data,
                model_name="dim_customer",
                connection=mock_connection,
                strategy_name="scd_type_2",
            )

        # For scd_type_2, a temp table SHOULD be created
        assert is_ref is True, (
            "prepare_reference_table should create a reference table for scd_type_2 "
            "strategy (needs_temp_table() returns True)"
        )
        # Verify create_table was called to create the temp table
        mock_connection.create_table.assert_called()
