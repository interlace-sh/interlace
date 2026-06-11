"""
Tests for the @check decorator and PythonCheck wrapper.

Tests the decorator metadata storage, registry management,
and PythonCheck return type handling (bool, int, ibis.Table).
"""

from unittest.mock import MagicMock

import ibis
import pytest

from interlace.checks.base import CheckSeverity, CheckStatus
from interlace.checks.decorator import PythonCheck, check, clear_check_registry, get_registered_checks


@pytest.fixture(autouse=True)
def _clean_registry():
    """Clear the check registry before and after each test."""
    clear_check_registry()
    yield
    clear_check_registry()


@pytest.mark.unit
class TestCheckDecorator:
    """Tests for the @check decorator."""

    def test_decorator_stores_metadata(self):
        """Decorating a function stores correct metadata on the function object."""

        @check(name="test_check", model="users", severity="warn", description="A test check")
        def my_check(table, connection):  # type: ignore[no-untyped-def]
            return True

        meta = my_check._interlace_check  # type: ignore[attr-defined]
        assert meta["name"] == "test_check"
        assert meta["model"] == ["users"]
        assert meta["severity"] == "warn"
        assert meta["description"] == "A test check"
        assert meta["function"] is my_check

    def test_decorator_registers_check(self):
        """Decorated function is added to the global check registry."""
        assert get_registered_checks() == []

        @check(name="registered_check", model="orders")
        def my_check(table, connection):  # type: ignore[no-untyped-def]
            return True

        registered = get_registered_checks()
        assert len(registered) == 1
        assert registered[0]["name"] == "registered_check"
        assert registered[0]["model"] == ["orders"]

    def test_decorator_multiple_models(self):
        """Passing a list of models stores all of them."""

        @check(name="multi_model_check", model=["orders", "returns"])
        def my_check(table, connection):  # type: ignore[no-untyped-def]
            return True

        meta = my_check._interlace_check  # type: ignore[attr-defined]
        assert meta["model"] == ["orders", "returns"]

    def test_decorator_default_severity(self):
        """Default severity is 'error' when not specified."""

        @check(name="default_sev", model="users")
        def my_check(table, connection):  # type: ignore[no-untyped-def]
            return True

        meta = my_check._interlace_check  # type: ignore[attr-defined]
        assert meta["severity"] == "error"

    def test_decorator_description_from_docstring(self):
        """Description falls back to the function docstring when not provided."""

        @check(name="docstring_check", model="users")
        def my_check(table, connection):  # type: ignore[no-untyped-def]
            """Check that uses its docstring."""
            return True

        meta = my_check._interlace_check  # type: ignore[attr-defined]
        assert meta["description"] == "Check that uses its docstring."


@pytest.mark.unit
class TestPythonCheckBool:
    """Tests for PythonCheck with bool return values."""

    def test_python_check_returns_true(self):
        """A function returning True produces a PASSED result."""

        def check_fn(table, connection):  # type: ignore[no-untyped-def]
            return True

        pc = PythonCheck(func=check_fn, name="bool_pass")
        mock_conn = MagicMock(spec=ibis.BaseBackend)
        mock_table = MagicMock(spec=ibis.Table)
        mock_conn.table.return_value = mock_table

        result = pc.run(mock_conn, "users")
        assert result.status == CheckStatus.PASSED

    def test_python_check_returns_false(self):
        """A function returning False produces a FAILED result."""

        def check_fn(table, connection):  # type: ignore[no-untyped-def]
            return False

        pc = PythonCheck(func=check_fn, name="bool_fail")
        mock_conn = MagicMock(spec=ibis.BaseBackend)
        mock_table = MagicMock(spec=ibis.Table)
        mock_conn.table.return_value = mock_table

        result = pc.run(mock_conn, "users")
        assert result.status == CheckStatus.FAILED


@pytest.mark.unit
class TestPythonCheckInt:
    """Tests for PythonCheck with int return values."""

    def test_python_check_returns_zero(self):
        """A function returning 0 (zero failures) produces a PASSED result."""

        def check_fn(table, connection):  # type: ignore[no-untyped-def]
            return 0

        pc = PythonCheck(func=check_fn, name="int_pass")
        mock_conn = MagicMock(spec=ibis.BaseBackend)
        mock_table = MagicMock(spec=ibis.Table)
        mock_conn.table.return_value = mock_table

        result = pc.run(mock_conn, "users")
        assert result.status == CheckStatus.PASSED
        assert result.failed_rows == 0

    def test_python_check_returns_nonzero(self):
        """A function returning a positive int (failure count) produces a FAILED result."""

        def check_fn(table, connection):  # type: ignore[no-untyped-def]
            return 3

        pc = PythonCheck(func=check_fn, name="int_fail")
        mock_conn = MagicMock(spec=ibis.BaseBackend)
        mock_table = MagicMock(spec=ibis.Table)
        mock_conn.table.return_value = mock_table

        result = pc.run(mock_conn, "users")
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows == 3


@pytest.mark.integration
class TestPythonCheckIbisTable:
    """Tests for PythonCheck with ibis.Table return values using a real DuckDB backend."""

    def test_python_check_returns_empty_table(self):
        """Returning an ibis.Table with 0 rows produces a PASSED result."""
        conn = ibis.duckdb.connect()
        conn.raw_sql("CREATE TABLE test_users (id INTEGER, name VARCHAR)")

        def check_fn(table, connection):  # type: ignore[no-untyped-def]
            # Return rows where id < 0 -- none exist, so 0 failing rows
            return table.filter(table["id"] < 0)

        pc = PythonCheck(func=check_fn, name="ibis_pass")
        result = pc.run(conn, "test_users")
        assert result.status == CheckStatus.PASSED
        assert result.failed_rows == 0

    def test_python_check_returns_nonempty_table(self):
        """Returning an ibis.Table with rows produces a FAILED result."""
        conn = ibis.duckdb.connect()
        conn.raw_sql("CREATE TABLE test_orders (id INTEGER, total DOUBLE)")
        conn.raw_sql("INSERT INTO test_orders VALUES (1, -5.0), (2, 10.0), (3, -1.0)")

        def check_fn(table, connection):  # type: ignore[no-untyped-def]
            # Return rows with negative totals
            return table.filter(table["total"] < 0)

        pc = PythonCheck(func=check_fn, name="ibis_fail")
        result = pc.run(conn, "test_orders")
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows == 2


@pytest.mark.unit
class TestPythonCheckErrorHandling:
    """Tests for PythonCheck error handling."""

    def test_python_check_error_handling(self):
        """A function that raises an exception produces an ERROR result."""

        def check_fn(table, connection):  # type: ignore[no-untyped-def]
            raise RuntimeError("something went wrong")

        pc = PythonCheck(func=check_fn, name="error_check")
        mock_conn = MagicMock(spec=ibis.BaseBackend)
        mock_table = MagicMock(spec=ibis.Table)
        mock_conn.table.return_value = mock_table

        result = pc.run(mock_conn, "users")
        assert result.status == CheckStatus.ERROR
        assert "something went wrong" in result.message

    def test_python_check_severity_propagated(self):
        """The severity set on PythonCheck is reflected in the result."""

        def check_fn(table, connection):  # type: ignore[no-untyped-def]
            return False

        pc = PythonCheck(func=check_fn, name="warn_check", severity=CheckSeverity.WARN)
        mock_conn = MagicMock(spec=ibis.BaseBackend)
        mock_table = MagicMock(spec=ibis.Table)
        mock_conn.table.return_value = mock_table

        result = pc.run(mock_conn, "users")
        assert result.severity == CheckSeverity.WARN
        assert result.status == CheckStatus.FAILED
