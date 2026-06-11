"""
Unit tests — test_model_sync() and mock_dependency() patterns.

Demonstrates how to test models in isolation with mock data,
without needing a database or config.yaml.
"""

import pytest

from interlace import mock_dependency, test_model_sync

from models.analytics import order_totals


# ---------------------------------------------------------------------------
# Fixtures — reusable mock data
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_orders():
    """A small set of order rows as a list of dicts."""
    return [
        {"order_id": 1, "product_id": 101, "quantity": 2, "order_date": "2024-01-15"},
        {"order_id": 2, "product_id": 102, "quantity": 1, "order_date": "2024-01-15"},
        {"order_id": 3, "product_id": 101, "quantity": 5, "order_date": "2024-01-16"},
    ]


@pytest.fixture
def sample_inventory():
    """A small inventory catalogue."""
    return [
        {"product_id": 101, "product_name": "Widget A", "price": 9.99, "stock_quantity": 150},
        {"product_id": 102, "product_name": "Widget B", "price": 19.99, "stock_quantity": 30},
    ]


# ---------------------------------------------------------------------------
# Basic test — pass raw dicts, inspect TestResult
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestOrderTotals:
    """Tests for the order_totals model."""

    def test_basic_join(self, sample_orders, sample_inventory):
        """Order totals should produce one row per matched order line."""
        result = test_model_sync(
            order_totals,
            deps={
                "orders": sample_orders,
                "inventory": sample_inventory,
            },
        )

        assert result.status == "success"
        assert result.error is None
        assert result.row_count == 3
        assert result.duration > 0

        # Verify expected columns
        expected_columns = {"order_id", "product_id", "product_name", "quantity", "price", "order_date", "total"}
        assert set(result.columns) == expected_columns

    def test_total_calculation(self, sample_orders, sample_inventory):
        """Verify that total = quantity * price for each row."""
        result = test_model_sync(
            order_totals,
            deps={
                "orders": sample_orders,
                "inventory": sample_inventory,
            },
        )

        rows = result.rows
        for row in rows:
            assert row["total"] == pytest.approx(row["quantity"] * row["price"])

    def test_dataframe_access(self, sample_orders, sample_inventory):
        """The .df property should return a pandas DataFrame."""
        result = test_model_sync(
            order_totals,
            deps={
                "orders": sample_orders,
                "inventory": sample_inventory,
            },
        )

        df = result.df
        assert df is not None
        assert len(df) == 3
        assert "total" in df.columns

    def test_rows_property(self, sample_orders, sample_inventory):
        """The .rows property should return a list of dicts."""
        result = test_model_sync(
            order_totals,
            deps={
                "orders": sample_orders,
                "inventory": sample_inventory,
            },
        )

        rows = result.rows
        assert isinstance(rows, list)
        assert len(rows) == 3
        assert all(isinstance(r, dict) for r in rows)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestEdgeCases:
    """Edge case tests to show how test_model_sync handles unusual inputs."""

    def test_empty_orders(self, sample_inventory):
        """An empty orders table should produce zero rows."""
        result = test_model_sync(
            order_totals,
            deps={
                "orders": [],
                "inventory": sample_inventory,
            },
            fields={
                "orders": {"order_id": "int64", "product_id": "int64", "quantity": "int64", "order_date": "string"},
            },
        )

        assert result.status == "success"
        assert result.row_count == 0

    def test_no_matching_products(self):
        """Orders referencing products not in inventory should produce no rows (inner join)."""
        result = test_model_sync(
            order_totals,
            deps={
                "orders": [
                    {"order_id": 1, "product_id": 999, "quantity": 1, "order_date": "2024-01-01"},
                ],
                "inventory": [
                    {"product_id": 101, "product_name": "Widget A", "price": 9.99, "stock_quantity": 150},
                ],
            },
        )

        assert result.status == "success"
        assert result.row_count == 0


# ---------------------------------------------------------------------------
# mock_dependency — explicit table creation
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestMockDependency:
    """Show how mock_dependency() can be used directly to create ibis.Table objects."""

    def test_explicit_mock(self):
        """Create mock tables manually and pass them to test_model_sync."""
        orders_table = mock_dependency(
            [
                {"order_id": 10, "product_id": 101, "quantity": 4, "order_date": "2024-02-01"},
            ],
        )
        inventory_table = mock_dependency(
            [
                {"product_id": 101, "product_name": "Widget A", "price": 9.99, "stock_quantity": 150},
            ],
        )

        result = test_model_sync(
            order_totals,
            deps={
                "orders": orders_table,
                "inventory": inventory_table,
            },
        )

        assert result.status == "success"
        assert result.row_count == 1
        assert result.rows[0]["total"] == pytest.approx(4 * 9.99)

    def test_mock_with_fields(self):
        """mock_dependency() accepts a fields dict to enforce column types."""
        table = mock_dependency(
            [{"product_id": 101, "product_name": "Widget A", "price": 9.99, "stock_quantity": 150}],
            fields={"product_id": "int64", "price": "float64"},
        )

        assert "product_id" in table.columns
        assert "price" in table.columns
