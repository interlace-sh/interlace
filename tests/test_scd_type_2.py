"""
Tests for SCD Type 2 strategy.

Tests hash-based change detection and history tracking.
"""

from unittest.mock import Mock

import ibis


class TestSCDType2Strategy:
    """Test SCD Type 2 strategy SQL generation."""

    def test_generate_sql_basic(self):
        """Test basic SCD2 SQL generation."""
        from interlace.strategies.scd_type_2 import SCDType2Strategy

        strategy = SCDType2Strategy()

        # Mock connection with schema
        mock_connection = Mock()
        mock_table = Mock()
        mock_schema = {
            "customer_id": ibis.dtype("int64"),
            "name": ibis.dtype("string"),
            "email": ibis.dtype("string"),
        }
        mock_table.schema.return_value = mock_schema
        mock_connection.table.return_value = mock_table

        sql = strategy.generate_sql(
            connection=mock_connection,
            target_table="dim_customer",
            schema="warehouse",
            source_table="_interlace_tmp_dim_customer",
            primary_key="customer_id",
        )

        # Verify SQL contains expected components
        assert "UPDATE" in sql
        assert "INSERT INTO" in sql
        assert "warehouse" in sql
        assert "dim_customer" in sql
        assert "valid_from" in sql
        assert "valid_to" in sql
        assert "is_current" in sql
        assert "_scd2_hash" in sql
        assert "MD5" in sql

    def test_generate_sql_with_tracked_columns(self):
        """Test SCD2 with specific tracked columns."""
        from interlace.strategies.scd_type_2 import SCDType2Strategy

        strategy = SCDType2Strategy()

        mock_connection = Mock()
        mock_table = Mock()
        mock_schema = {
            "customer_id": ibis.dtype("int64"),
            "name": ibis.dtype("string"),
            "email": ibis.dtype("string"),
            "phone": ibis.dtype("string"),
        }
        mock_table.schema.return_value = mock_schema
        mock_connection.table.return_value = mock_table

        sql = strategy.generate_sql(
            connection=mock_connection,
            target_table="dim_customer",
            schema="warehouse",
            source_table="_tmp",
            primary_key="customer_id",
            scd2_config={
                "tracked_columns": ["name", "email"],  # Only track name and email
            },
        )

        # Hash should be based on tracked columns only
        assert "name" in sql
        assert "email" in sql

    def test_generate_sql_with_custom_columns(self):
        """Test SCD2 with custom column names."""
        from interlace.strategies.scd_type_2 import SCDType2Strategy

        strategy = SCDType2Strategy()

        mock_connection = Mock()
        mock_table = Mock()
        mock_schema = {
            "id": ibis.dtype("int64"),
            "value": ibis.dtype("string"),
        }
        mock_table.schema.return_value = mock_schema
        mock_connection.table.return_value = mock_table

        sql = strategy.generate_sql(
            connection=mock_connection,
            target_table="test",
            schema="public",
            source_table="_tmp",
            primary_key="id",
            scd2_config={
                "valid_from_col": "effective_date",
                "valid_to_col": "expiration_date",
                "is_current_col": "is_active",
            },
        )

        # Custom column names should be used
        assert "effective_date" in sql
        assert "expiration_date" in sql
        assert "is_active" in sql

    def test_generate_sql_with_soft_delete(self):
        """Test SCD2 with soft delete mode."""
        from interlace.strategies.scd_type_2 import SCDType2Strategy

        strategy = SCDType2Strategy()

        mock_connection = Mock()
        mock_table = Mock()
        mock_schema = {
            "id": ibis.dtype("int64"),
            "value": ibis.dtype("string"),
        }
        mock_table.schema.return_value = mock_schema
        mock_connection.table.return_value = mock_table

        sql = strategy.generate_sql(
            connection=mock_connection,
            target_table="test",
            schema="public",
            source_table="_tmp",
            primary_key="id",
            scd2_config={
                "delete_mode": "soft",
            },
        )

        # Should have soft delete statement
        assert "NOT EXISTS" in sql
        statements = sql.split(";")
        # Should have 4 statements: expire, insert changed, insert new, soft delete
        assert len([s for s in statements if s.strip()]) == 4

    def test_needs_temp_table(self):
        """Test that SCD2 strategy requires temp table."""
        from interlace.strategies.scd_type_2 import SCDType2Strategy

        strategy = SCDType2Strategy()
        assert strategy.needs_temp_table() is True

    def test_get_required_columns(self):
        """Test getting required SCD2 columns."""
        from interlace.strategies.scd_type_2 import SCDType2Strategy

        strategy = SCDType2Strategy()

        # Default columns
        columns = strategy.get_required_columns()
        assert "valid_from" in columns
        assert "valid_to" in columns
        assert "is_current" in columns
        assert "_scd2_hash" in columns

        # With soft_mark delete mode
        columns = strategy.get_required_columns({"delete_mode": "soft_mark"})
        assert "is_deleted" in columns

    def test_build_hash_expression(self):
        """Test hash expression building."""
        from interlace.strategies.scd_type_2 import SCDType2Strategy

        strategy = SCDType2Strategy()

        hash_expr = strategy._build_hash_expression(
            columns=["name", "email", "phone"],
            table_alias="source",
        )

        assert "MD5" in hash_expr
        assert "source" in hash_expr
        assert "name" in hash_expr
        assert "email" in hash_expr
        assert "phone" in hash_expr
        assert "COALESCE" in hash_expr  # NULL handling

    def test_initial_insert_sql(self):
        """Test initial load SQL generation."""
        from interlace.strategies.scd_type_2 import SCDType2Strategy

        strategy = SCDType2Strategy()

        mock_connection = Mock()
        mock_table = Mock()
        mock_schema = {
            "customer_id": ibis.dtype("int64"),
            "name": ibis.dtype("string"),
        }
        mock_table.schema.return_value = mock_schema
        mock_connection.table.return_value = mock_table

        sql = strategy.get_initial_insert_sql(
            connection=mock_connection,
            target_table="dim_customer",
            schema="warehouse",
            source_table="_tmp",
            primary_key="customer_id",
        )

        # Initial insert should set all records as current
        assert "INSERT INTO" in sql
        assert "CURRENT_TIMESTAMP" in sql
        assert "NULL" in sql  # valid_to should be NULL
        assert "TRUE" in sql  # is_current should be TRUE


class TestSCDType2StrategyRegistration:
    """Test SCD Type 2 strategy is properly registered."""

    def test_strategy_registered(self):
        """Test SCD2 strategy is in the registry."""
        from interlace.strategies import STRATEGIES

        assert "scd_type_2" in STRATEGIES

    def test_get_strategy(self):
        """Test getting SCD2 strategy by name."""
        from interlace.strategies import get_strategy

        strategy = get_strategy("scd_type_2")
        assert strategy is not None
        assert hasattr(strategy, "generate_sql")
        assert hasattr(strategy, "needs_temp_table")
