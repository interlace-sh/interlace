"""
Tests for the interlace testing framework (test_model, mock_dependency, TestResult).
"""

import ibis
import pytest

from interlace.core.model import model
from interlace.testing import TestResult, mock_dependency, test_model, test_model_sync

# ---------------------------------------------------------------------------
# mock_dependency
# ---------------------------------------------------------------------------


class TestMockDependency:
    """Tests for mock_dependency helper."""

    def test_list_of_dicts(self):
        """Create ibis.Table from list of dicts."""
        table = mock_dependency([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        assert isinstance(table, ibis.Table)
        assert set(table.columns) == {"id", "name"}
        assert table.count().execute() == 2

    def test_dict_of_lists(self):
        """Create ibis.Table from dict of lists."""
        table = mock_dependency({"id": [1, 2], "name": ["Alice", "Bob"]})
        assert isinstance(table, ibis.Table)
        assert table.count().execute() == 2

    def test_with_fields(self):
        """Fields parameter overrides inferred types."""
        table = mock_dependency(
            [{"id": 1, "name": "Alice"}],
            fields={"id": "int64", "name": "string"},
        )
        schema = table.schema()
        assert "int64" in str(schema["id"]).lower()
        assert "string" in str(schema["name"]).lower()

    def test_strict_mode(self):
        """Strict mode drops columns not in fields."""
        table = mock_dependency(
            [{"id": 1, "name": "Alice", "extra": "ignored"}],
            fields={"id": "int64", "name": "string"},
            strict=True,
        )
        assert "extra" not in table.columns
        assert set(table.columns) == {"id", "name"}

    def test_single_dict(self):
        """Single dict is treated as a single row."""
        table = mock_dependency({"id": 1, "name": "Alice"})
        assert table.count().execute() == 1

    def test_ibis_table_passthrough(self):
        """ibis.Table is passed through unchanged."""
        original = ibis.memtable([{"x": 1}])
        result = mock_dependency(original)
        # Should be the same object (ibis.Table → returned as-is via DataConverter)
        assert isinstance(result, ibis.Table)


# ---------------------------------------------------------------------------
# test_model_sync
# ---------------------------------------------------------------------------


class TestTestModelSync:
    """Tests for test_model_sync helper."""

    def test_simple_model(self):
        """Test a simple model that joins two tables."""

        def enriched(users, orders):
            return users.join(orders, users.id == orders.user_id).select(users.id, users.name, orders.amount)

        result = test_model_sync(
            enriched,
            deps={
                "users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
                "orders": [{"id": 1, "user_id": 1, "amount": 42.0}],
            },
        )

        assert result.status == "success"
        assert result.error is None
        assert result.row_count == 1
        assert "name" in result.columns
        assert "amount" in result.columns

    def test_model_returning_list(self):
        """Test a model that returns list of dicts (not ibis.Table)."""

        def api_model():
            return [{"key": "a", "value": 1}, {"key": "b", "value": 2}]

        result = test_model_sync(api_model, deps={})
        assert result.status == "success"
        assert result.row_count == 2
        assert set(result.columns) == {"key", "value"}

    def test_model_returning_none(self):
        """Test a side-effect model that returns None."""

        def side_effect_model(data):
            # Do nothing, return None
            pass

        result = test_model_sync(
            side_effect_model,
            deps={
                "data": [{"x": 1}],
            },
        )
        assert result.status == "success"
        assert result.table is None
        assert result.row_count is None

    def test_model_error(self):
        """Test a model that raises an error."""

        def bad_model(data):
            raise ValueError("Something went wrong")

        result = test_model_sync(
            bad_model,
            deps={
                "data": [{"x": 1}],
            },
        )
        assert result.status == "error"
        assert "Something went wrong" in result.error

    def test_decorated_model(self):
        """Test a @model-decorated function."""

        @model(name="filtered", materialise="table")
        def filtered(users):
            return users.filter(users.active)

        result = test_model_sync(
            filtered,
            deps={
                "users": [
                    {"id": 1, "name": "Alice", "active": True},
                    {"id": 2, "name": "Bob", "active": False},
                ],
            },
        )
        assert result.status == "success"
        assert result.row_count == 1

    def test_duration_tracked(self):
        """Duration is tracked in the result."""

        def quick_model():
            return [{"x": 1}]

        result = test_model_sync(quick_model, deps={})
        assert result.duration >= 0.0

    def test_rows_property(self):
        """rows property returns list of dicts."""

        def simple():
            return [{"a": 1, "b": "hello"}]

        result = test_model_sync(simple, deps={})
        rows = result.rows
        assert len(rows) == 1
        assert rows[0]["a"] == 1
        assert rows[0]["b"] == "hello"

    def test_df_property(self):
        """df property returns pandas DataFrame."""

        def simple():
            return [{"x": 10, "y": 20}]

        result = test_model_sync(simple, deps={})
        df = result.df
        assert len(df) == 1
        assert df["x"].iloc[0] == 10

    def test_per_dependency_fields(self):
        """Fields can be specified per dependency."""

        def passthrough(data):
            return data

        result = test_model_sync(
            passthrough,
            deps={"data": [{"val": "42"}]},
            fields={"data": {"val": "string"}},
        )
        assert result.status == "success"
        schema = result.table.schema()
        assert "string" in str(schema["val"]).lower()


# ---------------------------------------------------------------------------
# test_model (async)
# ---------------------------------------------------------------------------


class TestTestModelAsync:
    """Tests for async test_model helper."""

    @pytest.mark.asyncio
    async def test_async_model(self):
        """Test an async model function."""

        async def async_enriched(users):
            return users.filter(users.id > 0)

        result = await test_model(
            async_enriched,
            deps={
                "users": [{"id": 1, "name": "Alice"}, {"id": -1, "name": "Ghost"}],
            },
        )
        assert result.status == "success"
        assert result.row_count == 1


# ---------------------------------------------------------------------------
# TestResult dataclass
# ---------------------------------------------------------------------------


class TestTestResult:
    """Tests for TestResult dataclass."""

    def test_default_state(self):
        """Default TestResult has success status and no table."""
        r = TestResult()
        assert r.status == "success"
        assert r.table is None
        assert r.error is None
        assert r.row_count is None
        assert r.columns == []
        assert r.rows == []

    def test_error_state(self):
        """Error TestResult preserves error message."""
        r = TestResult(status="error", error="boom")
        assert r.status == "error"
        assert r.error == "boom"
        assert r.row_count is None
