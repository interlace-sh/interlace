"""
Async tests — test_model() patterns for async models.

Uses the async version of test_model() with pytest asyncio_mode = "auto".
"""

import pytest

from interlace import test_model

from models.analytics import low_stock_alerts


# ---------------------------------------------------------------------------
# Async model tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestLowStockAlerts:
    """Tests for the async low_stock_alerts model."""

    async def test_filters_low_stock(self):
        """Products with stock_quantity < 10 should be flagged."""
        result = await test_model(
            low_stock_alerts,
            deps={
                "inventory": [
                    {"product_id": 101, "product_name": "Widget A", "price": 9.99, "stock_quantity": 150},
                    {"product_id": 102, "product_name": "Widget B", "price": 19.99, "stock_quantity": 5},
                    {"product_id": 103, "product_name": "Gadget C", "price": 49.99, "stock_quantity": 8},
                    {"product_id": 104, "product_name": "Gadget D", "price": 29.99, "stock_quantity": 200},
                ],
            },
        )

        assert result.status == "success"
        assert result.row_count == 2

        product_names = {row["product_name"] for row in result.rows}
        assert product_names == {"Widget B", "Gadget C"}

    async def test_no_low_stock(self):
        """When all products have sufficient stock, no alerts should be raised."""
        result = await test_model(
            low_stock_alerts,
            deps={
                "inventory": [
                    {"product_id": 101, "product_name": "Widget A", "price": 9.99, "stock_quantity": 100},
                    {"product_id": 102, "product_name": "Widget B", "price": 19.99, "stock_quantity": 50},
                ],
            },
        )

        assert result.status == "success"
        assert result.row_count == 0

    async def test_all_low_stock(self):
        """When every product is below threshold, all should appear."""
        result = await test_model(
            low_stock_alerts,
            deps={
                "inventory": [
                    {"product_id": 101, "product_name": "Widget A", "price": 9.99, "stock_quantity": 3},
                    {"product_id": 102, "product_name": "Widget B", "price": 19.99, "stock_quantity": 1},
                ],
            },
        )

        assert result.status == "success"
        assert result.row_count == 2

    async def test_boundary_value(self):
        """stock_quantity == 10 should NOT trigger an alert (threshold is < 10)."""
        result = await test_model(
            low_stock_alerts,
            deps={
                "inventory": [
                    {"product_id": 101, "product_name": "Widget A", "price": 9.99, "stock_quantity": 10},
                    {"product_id": 102, "product_name": "Widget B", "price": 19.99, "stock_quantity": 9},
                ],
            },
        )

        assert result.status == "success"
        assert result.row_count == 1
        assert result.rows[0]["product_name"] == "Widget B"

    async def test_result_columns(self):
        """Verify the alert output retains all inventory columns."""
        result = await test_model(
            low_stock_alerts,
            deps={
                "inventory": [
                    {"product_id": 101, "product_name": "Widget A", "price": 9.99, "stock_quantity": 3},
                ],
            },
        )

        expected_columns = {"product_id", "product_name", "price", "stock_quantity"}
        assert set(result.columns) == expected_columns
