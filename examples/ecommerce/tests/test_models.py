"""Unit tests for ecommerce models using interlace.testing.

Demonstrates: test_model_sync(), mock_dependency(), TestResult properties.

Note: Source models (customers, products, etc.) use get_connection() and
require a live database — test them via integration tests or `interlace run`.
Transform models that accept ibis.Table parameters are ideal for unit testing.
"""

import pytest
from interlace import test_model_sync, mock_dependency

from models.staging import stg_orders
from models.analytics import customer_orders


class TestStaging:
    """Test staging models filter correctly."""

    @pytest.mark.unit
    def test_stg_orders_filters_cancelled(self):
        mock_orders = mock_dependency(
            [
                {"order_id": 1, "customer_id": 1, "product_id": 101, "quantity": 1, "order_date": "2024-01-01", "status": "completed"},
                {"order_id": 2, "customer_id": 2, "product_id": 102, "quantity": 2, "order_date": "2024-01-02", "status": "cancelled"},
                {"order_id": 3, "customer_id": 3, "product_id": 103, "quantity": 1, "order_date": "2024-01-03", "status": "completed"},
            ]
        )
        result = test_model_sync(stg_orders, deps={"orders": mock_orders})
        assert result.row_count == 2
        for row in result.rows:
            assert row["status"] == "completed"


class TestAnalytics:
    """Test analytics models join and aggregate correctly."""

    @pytest.mark.unit
    def test_customer_orders_join(self):
        mock_customers = mock_dependency(
            [
                {"customer_id": 1, "name": "Alice", "email": "alice@test.com", "segment": "premium", "created_at": "2024-01-01"},
            ]
        )
        mock_stg_orders = mock_dependency(
            [
                {"order_id": 100, "customer_id": 1, "product_id": 101, "quantity": 2, "order_date": "2024-01-05", "status": "completed"},
                {"order_id": 101, "customer_id": 1, "product_id": 102, "quantity": 1, "order_date": "2024-01-06", "status": "completed"},
            ]
        )
        result = test_model_sync(
            customer_orders,
            deps={"customers": mock_customers, "stg_orders": mock_stg_orders},
        )
        assert result.row_count == 2
        assert "name" in result.columns
        assert "order_id" in result.columns
        assert all(row["name"] == "Alice" for row in result.rows)
