"""
Tests for the data quality checks framework.

Phase 3: Tests for quality checks and runner.
"""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd

from interlace.quality.base import (
    QualityCheck,
    QualityCheckResult,
    QualityCheckSeverity,
    QualityCheckStatus,
)
from interlace.quality.checks.unique import UniqueCheck
from interlace.quality.checks.not_null import NotNullCheck
from interlace.quality.checks.accepted_values import AcceptedValuesCheck
from interlace.quality.checks.freshness import FreshnessCheck
from interlace.quality.checks.row_count import RowCountCheck
from interlace.quality.checks.expression import ExpressionCheck
from interlace.quality.runner import QualityCheckRunner, QualityCheckSummary


class TestQualityCheckResult:
    """Tests for QualityCheckResult."""

    def test_passed_property(self):
        """Test passed property."""
        result = QualityCheckResult(
            check_name="test",
            check_type="unique",
            status=QualityCheckStatus.PASSED,
            severity=QualityCheckSeverity.ERROR,
            table_name="users",
        )
        assert result.passed is True

        result2 = QualityCheckResult(
            check_name="test",
            check_type="unique",
            status=QualityCheckStatus.FAILED,
            severity=QualityCheckSeverity.ERROR,
            table_name="users",
        )
        assert result2.passed is False

    def test_failure_rate(self):
        """Test failure rate calculation."""
        result = QualityCheckResult(
            check_name="test",
            check_type="unique",
            status=QualityCheckStatus.FAILED,
            severity=QualityCheckSeverity.ERROR,
            table_name="users",
            failed_rows=25,
            total_rows=100,
        )
        assert result.failure_rate == 25.0

    def test_failure_rate_zero_rows(self):
        """Test failure rate with zero rows."""
        result = QualityCheckResult(
            check_name="test",
            check_type="unique",
            status=QualityCheckStatus.PASSED,
            severity=QualityCheckSeverity.ERROR,
            table_name="users",
            failed_rows=0,
            total_rows=0,
        )
        assert result.failure_rate == 0.0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        result = QualityCheckResult(
            check_name="test",
            check_type="unique",
            status=QualityCheckStatus.PASSED,
            severity=QualityCheckSeverity.ERROR,
            table_name="users",
        )
        d = result.to_dict()
        assert d["check_name"] == "test"
        assert d["status"] == "passed"
        assert d["severity"] == "error"


class TestUniqueCheck:
    """Tests for UniqueCheck."""

    def test_requires_column(self):
        """Test that column is required."""
        with pytest.raises(ValueError, match="requires at least one column"):
            UniqueCheck()

    def test_check_type(self):
        """Test check_type property."""
        check = UniqueCheck(column="user_id")
        assert check.check_type == "unique"

    def test_check_name_single_column(self):
        """Test check name with single column."""
        check = UniqueCheck(column="user_id")
        assert check.check_name == "unique_user_id"

    def test_check_name_multiple_columns(self):
        """Test check name with multiple columns."""
        check = UniqueCheck(columns=["tenant_id", "user_id"])
        assert check.check_name == "unique_tenant_id_user_id"

    def test_check_name_custom(self):
        """Test custom check name."""
        check = UniqueCheck(column="user_id", name="custom_unique")
        assert check.check_name == "custom_unique"


class TestNotNullCheck:
    """Tests for NotNullCheck."""

    def test_requires_column(self):
        """Test that column is required."""
        with pytest.raises(ValueError, match="requires a column"):
            NotNullCheck(column=None)

    def test_check_type(self):
        """Test check_type property."""
        check = NotNullCheck(column="email")
        assert check.check_type == "not_null"

    def test_check_name(self):
        """Test check name."""
        check = NotNullCheck(column="email")
        assert check.check_name == "not_null_email"


class TestAcceptedValuesCheck:
    """Tests for AcceptedValuesCheck."""

    def test_requires_column(self):
        """Test that column is required."""
        with pytest.raises(ValueError, match="requires a column"):
            AcceptedValuesCheck(column=None, values=["a", "b"])

    def test_requires_values(self):
        """Test that values are required."""
        with pytest.raises(ValueError, match="requires at least one"):
            AcceptedValuesCheck(column="status", values=[])

    def test_check_type(self):
        """Test check_type property."""
        check = AcceptedValuesCheck(column="status", values=["active", "inactive"])
        assert check.check_type == "accepted_values"


class TestFreshnessCheck:
    """Tests for FreshnessCheck."""

    def test_requires_column(self):
        """Test that column is required."""
        with pytest.raises(ValueError, match="requires a column"):
            FreshnessCheck(column=None, max_age_hours=24)

    def test_requires_max_age(self):
        """Test that max age is required."""
        with pytest.raises(ValueError, match="requires max_age"):
            FreshnessCheck(column="updated_at")

    def test_check_type(self):
        """Test check_type property."""
        check = FreshnessCheck(column="updated_at", max_age_hours=24)
        assert check.check_type == "freshness"

    def test_max_age_hours(self):
        """Test max age in hours."""
        check = FreshnessCheck(column="updated_at", max_age_hours=24)
        assert check.max_age_hours == 24.0

    def test_max_age_days(self):
        """Test max age in days."""
        check = FreshnessCheck(column="updated_at", max_age_days=7)
        assert check.max_age_hours == 168.0  # 7 * 24

    def test_max_age_minutes(self):
        """Test max age in minutes."""
        check = FreshnessCheck(column="updated_at", max_age_minutes=30)
        assert check.max_age_hours == 0.5  # 30 / 60

    def test_max_age_combined(self):
        """Test combined max age."""
        check = FreshnessCheck(
            column="updated_at",
            max_age_days=1,
            max_age_hours=6,
            max_age_minutes=30,
        )
        assert check.max_age_hours == 30.5  # 24 + 6 + 0.5


class TestRowCountCheck:
    """Tests for RowCountCheck."""

    def test_requires_bounds(self):
        """Test that at least one bound is required."""
        with pytest.raises(ValueError, match="requires min_count"):
            RowCountCheck()

    def test_check_type(self):
        """Test check_type property."""
        check = RowCountCheck(min_count=100)
        assert check.check_type == "row_count"

    def test_check_name_min(self):
        """Test check name with min."""
        check = RowCountCheck(min_count=100)
        assert "min_100" in check.check_name

    def test_check_name_max(self):
        """Test check name with max."""
        check = RowCountCheck(max_count=1000)
        assert "max_1000" in check.check_name

    def test_check_name_both(self):
        """Test check name with both bounds."""
        check = RowCountCheck(min_count=100, max_count=1000)
        assert "min_100" in check.check_name
        assert "max_1000" in check.check_name


class TestExpressionCheck:
    """Tests for ExpressionCheck."""

    def test_requires_expression(self):
        """Test that expression is required."""
        with pytest.raises(ValueError, match="requires a callable"):
            ExpressionCheck(expression="not callable", name="test")

    def test_requires_name(self):
        """Test that name is required."""
        with pytest.raises(ValueError, match="requires a name"):
            ExpressionCheck(expression=lambda t: t["x"] > 0, name="")

    def test_check_type(self):
        """Test check_type property."""
        check = ExpressionCheck(expression=lambda t: t["x"] > 0, name="positive_x")
        assert check.check_type == "expression"

    def test_check_name(self):
        """Test check name."""
        check = ExpressionCheck(expression=lambda t: t["x"] > 0, name="positive_x")
        assert check.check_name == "positive_x"


class TestQualityCheckSummary:
    """Tests for QualityCheckSummary."""

    def test_has_failures_true(self):
        """Test has_failures with ERROR severity failure."""
        summary = QualityCheckSummary(table_name="users")
        summary.results.append(
            QualityCheckResult(
                check_name="test",
                check_type="unique",
                status=QualityCheckStatus.FAILED,
                severity=QualityCheckSeverity.ERROR,
                table_name="users",
            )
        )
        assert summary.has_failures is True

    def test_has_failures_false_with_warning(self):
        """Test has_failures is False with only WARN failures."""
        summary = QualityCheckSummary(table_name="users")
        summary.results.append(
            QualityCheckResult(
                check_name="test",
                check_type="unique",
                status=QualityCheckStatus.FAILED,
                severity=QualityCheckSeverity.WARN,
                table_name="users",
            )
        )
        assert summary.has_failures is False
        assert summary.has_warnings is True

    def test_success_rate(self):
        """Test success rate calculation."""
        summary = QualityCheckSummary(
            table_name="users",
            total_checks=10,
            passed=8,
            failed=2,
        )
        assert summary.success_rate == 80.0

    def test_success_rate_zero_checks(self):
        """Test success rate with zero checks."""
        summary = QualityCheckSummary(table_name="users")
        assert summary.success_rate == 100.0


class TestQualityCheckRunner:
    """Tests for QualityCheckRunner."""

    def test_requires_connection(self):
        """Test that connection is required."""
        runner = QualityCheckRunner()
        with pytest.raises(ValueError, match="No connection"):
            runner.run_checks("users", [])

    def test_parse_check_configs_unique(self):
        """Test parsing unique check config."""
        runner = QualityCheckRunner()
        configs = [{"type": "unique", "column": "user_id"}]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 1
        assert isinstance(checks[0], UniqueCheck)
        assert checks[0].column == "user_id"

    def test_parse_check_configs_not_null(self):
        """Test parsing not_null check config."""
        runner = QualityCheckRunner()
        configs = [{"type": "not_null", "column": "email"}]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 1
        assert isinstance(checks[0], NotNullCheck)

    def test_parse_check_configs_accepted_values(self):
        """Test parsing accepted_values check config."""
        runner = QualityCheckRunner()
        configs = [
            {
                "type": "accepted_values",
                "column": "status",
                "values": ["active", "inactive"],
            }
        ]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 1
        assert isinstance(checks[0], AcceptedValuesCheck)

    def test_parse_check_configs_freshness(self):
        """Test parsing freshness check config."""
        runner = QualityCheckRunner()
        configs = [
            {
                "type": "freshness",
                "column": "updated_at",
                "max_age_hours": 24,
            }
        ]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 1
        assert isinstance(checks[0], FreshnessCheck)

    def test_parse_check_configs_row_count(self):
        """Test parsing row_count check config."""
        runner = QualityCheckRunner()
        configs = [{"type": "row_count", "min_count": 100}]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 1
        assert isinstance(checks[0], RowCountCheck)

    def test_parse_check_configs_with_severity(self):
        """Test parsing check config with severity."""
        runner = QualityCheckRunner()
        configs = [
            {
                "type": "not_null",
                "column": "email",
                "severity": "warn",
            }
        ]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 1
        assert checks[0].severity == QualityCheckSeverity.WARN

    def test_parse_check_configs_unknown_type(self):
        """Test parsing config with unknown type."""
        runner = QualityCheckRunner()
        configs = [{"type": "unknown_check", "column": "x"}]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 0

    def test_parse_check_configs_missing_type(self):
        """Test parsing config without type."""
        runner = QualityCheckRunner()
        configs = [{"column": "x"}]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 0


class TestQualityCheckWithMockedConnection:
    """Tests that run quality checks with mocked ibis connection."""

    @pytest.fixture
    def mock_connection(self):
        """Create mock ibis connection."""
        conn = MagicMock()
        return conn

    @pytest.fixture
    def mock_table(self):
        """Create mock ibis table."""
        table = MagicMock()
        return table

    def test_unique_check_passes(self, mock_connection, mock_table):
        """Test unique check with no duplicates."""
        # Setup mock
        mock_connection.table.return_value = mock_table
        mock_table.count.return_value.execute.return_value = 100
        mock_table.__getitem__.return_value.nunique.return_value.execute.return_value = 100

        check = UniqueCheck(column="user_id")
        result = check.run(mock_connection, "users")

        assert result.status == QualityCheckStatus.PASSED
        assert result.failed_rows == 0

    def test_unique_check_fails(self, mock_connection, mock_table):
        """Test unique check with duplicates."""
        # Setup mock
        mock_connection.table.return_value = mock_table
        mock_table.count.return_value.execute.return_value = 100
        mock_table.__getitem__.return_value.nunique.return_value.execute.return_value = 90

        check = UniqueCheck(column="user_id")
        result = check.run(mock_connection, "users")

        assert result.status == QualityCheckStatus.FAILED
        assert result.failed_rows == 10  # 100 - 90 duplicates

    def test_not_null_check_passes(self, mock_connection, mock_table):
        """Test not_null check with no nulls."""
        # Setup mock
        mock_connection.table.return_value = mock_table
        mock_table.count.return_value.execute.return_value = 100
        mock_table.filter.return_value.count.return_value.execute.return_value = 0

        check = NotNullCheck(column="email")
        result = check.run(mock_connection, "users")

        assert result.status == QualityCheckStatus.PASSED
        assert result.failed_rows == 0

    def test_not_null_check_fails(self, mock_connection, mock_table):
        """Test not_null check with nulls."""
        # Setup mock
        mock_connection.table.return_value = mock_table
        mock_table.count.return_value.execute.return_value = 100
        mock_table.filter.return_value.count.return_value.execute.return_value = 5

        check = NotNullCheck(column="email")
        result = check.run(mock_connection, "users")

        assert result.status == QualityCheckStatus.FAILED
        assert result.failed_rows == 5

    def test_row_count_check_passes(self, mock_connection, mock_table):
        """Test row_count check passes."""
        # Setup mock
        mock_connection.table.return_value = mock_table
        mock_table.count.return_value.execute.return_value = 500

        check = RowCountCheck(min_count=100, max_count=1000)
        result = check.run(mock_connection, "users")

        assert result.status == QualityCheckStatus.PASSED

    def test_row_count_check_fails_below_min(self, mock_connection, mock_table):
        """Test row_count check fails below min."""
        # Setup mock
        mock_connection.table.return_value = mock_table
        mock_table.count.return_value.execute.return_value = 50

        check = RowCountCheck(min_count=100)
        result = check.run(mock_connection, "users")

        assert result.status == QualityCheckStatus.FAILED
        assert "below minimum" in result.message

    def test_row_count_check_fails_above_max(self, mock_connection, mock_table):
        """Test row_count check fails above max."""
        # Setup mock
        mock_connection.table.return_value = mock_table
        mock_table.count.return_value.execute.return_value = 2000

        check = RowCountCheck(max_count=1000)
        result = check.run(mock_connection, "users")

        assert result.status == QualityCheckStatus.FAILED
        assert "exceeds maximum" in result.message
