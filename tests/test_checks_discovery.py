"""
Tests for SQL and Python check discovery.

Tests annotation parsing, SQL body extraction, and file/registry-based
discovery of check definitions.
"""

from pathlib import Path

import pytest

from interlace.checks.base import CheckSeverity
from interlace.checks.decorator import check, clear_check_registry
from interlace.checks.discovery import (
    _extract_sql_body,
    _parse_check_annotations,
    discover_python_checks,
    discover_sql_checks,
)
from interlace.checks.types.sql import SqlCheck


@pytest.fixture(autouse=True)
def _clean_registry():
    """Clear the check registry before and after each test."""
    clear_check_registry()
    yield
    clear_check_registry()


@pytest.mark.unit
class TestParseCheckAnnotations:
    """Tests for _parse_check_annotations."""

    def test_parse_check_annotations(self):
        """All annotation key-value pairs are extracted from SQL comments."""
        content = "-- @name: test\n-- @model: users\n-- @severity: warn\nSELECT * FROM users WHERE id IS NULL;"
        annotations = _parse_check_annotations(content)
        assert annotations["name"] == "test"
        assert annotations["model"] == "users"
        assert annotations["severity"] == "warn"

    def test_parse_annotations_with_description(self):
        """Description annotation is extracted correctly."""
        content = "-- @name: check_1\n-- @model: orders\n-- @description: Ensure no nulls"
        annotations = _parse_check_annotations(content)
        assert annotations["description"] == "Ensure no nulls"

    def test_parse_annotations_empty_content(self):
        """Empty content returns an empty dict."""
        assert _parse_check_annotations("") == {}

    def test_parse_annotations_no_annotations(self):
        """SQL without annotations returns an empty dict."""
        content = "SELECT * FROM users WHERE active = true;"
        assert _parse_check_annotations(content) == {}


@pytest.mark.unit
class TestExtractSqlBody:
    """Tests for _extract_sql_body."""

    def test_extract_sql_body(self):
        """Annotation lines are stripped and the SQL body is preserved."""
        content = "-- @name: test\n-- @model: users\n-- @severity: warn\nSELECT * FROM users WHERE id IS NULL;"
        body = _extract_sql_body(content)
        assert body == "SELECT * FROM users WHERE id IS NULL;"
        assert "@name" not in body
        assert "@model" not in body
        assert "@severity" not in body

    def test_extract_sql_body_preserves_regular_comments(self):
        """Non-annotation SQL comments are kept."""
        content = "-- @name: test\n-- @model: users\n-- This is a regular comment\nSELECT 1;"
        body = _extract_sql_body(content)
        assert "-- This is a regular comment" in body
        assert "SELECT 1;" in body

    def test_extract_sql_body_strips_whitespace(self):
        """Leading/trailing whitespace is trimmed from the body."""
        content = "-- @name: test\n\n  SELECT 1;  \n\n"
        body = _extract_sql_body(content)
        assert body == "SELECT 1;"


@pytest.mark.unit
class TestDiscoverSqlChecks:
    """Tests for discover_sql_checks."""

    def test_discover_sql_checks(self, tmp_path: Path):
        """A valid .sql check file is discovered and mapped to its model."""
        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()
        sql_file = checks_dir / "no_null_emails.sql"
        sql_file.write_text(
            "-- @name: no_null_emails\n"
            "-- @model: users\n"
            "-- @severity: error\n"
            "SELECT * FROM {table} WHERE email IS NULL;\n"
        )

        result = discover_sql_checks(tmp_path)

        assert "users" in result
        assert len(result["users"]) == 1

        check_instance = result["users"][0]["_check_instance"]
        assert isinstance(check_instance, SqlCheck)
        assert check_instance.name == "no_null_emails"
        assert check_instance.severity == CheckSeverity.ERROR

    def test_discover_sql_checks_no_directory(self, tmp_path: Path):
        """Returns empty dict when no checks/ directory exists."""
        result = discover_sql_checks(tmp_path)
        assert result == {}

    def test_discover_sql_checks_missing_annotations(self, tmp_path: Path):
        """SQL files without required @name or @model annotations are skipped."""
        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()

        # Missing @name
        no_name = checks_dir / "no_name.sql"
        no_name.write_text("-- @model: users\nSELECT 1;\n")

        # Missing @model
        no_model = checks_dir / "no_model.sql"
        no_model.write_text("-- @name: orphan_check\nSELECT 1;\n")

        # Missing both
        no_both = checks_dir / "plain.sql"
        no_both.write_text("SELECT 1;\n")

        result = discover_sql_checks(tmp_path)
        assert result == {}

    def test_discover_sql_checks_multiple_files(self, tmp_path: Path):
        """Multiple check files for different models are all discovered."""
        checks_dir = tmp_path / "checks"
        checks_dir.mkdir()

        (checks_dir / "check_a.sql").write_text(
            "-- @name: check_a\n-- @model: users\nSELECT * FROM {table} WHERE id IS NULL;\n"
        )
        (checks_dir / "check_b.sql").write_text(
            "-- @name: check_b\n-- @model: orders\n-- @severity: warn\nSELECT * FROM {table} WHERE total < 0;\n"
        )

        result = discover_sql_checks(tmp_path)
        assert "users" in result
        assert "orders" in result
        assert len(result["users"]) == 1
        assert len(result["orders"]) == 1

        orders_check = result["orders"][0]["_check_instance"]
        assert orders_check.severity == CheckSeverity.WARN

    def test_discover_sql_checks_nested_directory(self, tmp_path: Path):
        """Check files in subdirectories of checks/ are discovered."""
        nested_dir = tmp_path / "checks" / "finance"
        nested_dir.mkdir(parents=True)

        (nested_dir / "revenue_check.sql").write_text(
            "-- @name: revenue_positive\n-- @model: revenue\nSELECT * FROM {table} WHERE amount <= 0;\n"
        )

        result = discover_sql_checks(tmp_path)
        assert "revenue" in result
        assert result["revenue"][0]["_check_instance"].name == "revenue_positive"


@pytest.mark.unit
class TestDiscoverPythonChecks:
    """Tests for discover_python_checks."""

    def test_discover_python_checks(self):
        """A registered @check function is discovered and wrapped as PythonCheck."""

        @check(name="py_check", model="users", severity="error")
        def my_check(table, connection):  # type: ignore[no-untyped-def]
            return True

        result = discover_python_checks()

        assert "users" in result
        assert len(result["users"]) == 1

        check_instance = result["users"][0]["_check_instance"]
        assert check_instance.name == "py_check"
        assert check_instance.severity == CheckSeverity.ERROR

    def test_discover_python_checks_multiple_models(self):
        """A check registered for multiple models appears under each model."""

        @check(name="shared_check", model=["orders", "returns"], severity="warn")
        def shared(table, connection):  # type: ignore[no-untyped-def]
            return True

        result = discover_python_checks()

        assert "orders" in result
        assert "returns" in result
        assert len(result["orders"]) == 1
        assert len(result["returns"]) == 1
        assert result["orders"][0]["_check_instance"].name == "shared_check"
        assert result["returns"][0]["_check_instance"].name == "shared_check"

    def test_discover_python_checks_empty_registry(self):
        """Returns empty dict when no checks are registered."""
        result = discover_python_checks()
        assert result == {}

    def test_discover_python_checks_severity_mapping(self):
        """Severity strings are mapped to the correct CheckSeverity enum values."""

        @check(name="warn_check", model="a", severity="warn")
        def warn_fn(table, connection):  # type: ignore[no-untyped-def]
            return True

        @check(name="info_check", model="b", severity="info")
        def info_fn(table, connection):  # type: ignore[no-untyped-def]
            return True

        result = discover_python_checks()
        assert result["a"][0]["_check_instance"].severity == CheckSeverity.WARN
        assert result["b"][0]["_check_instance"].severity == CheckSeverity.INFO
