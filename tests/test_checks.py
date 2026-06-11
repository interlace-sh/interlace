"""
Tests for the data checks framework.

Comprehensive unit tests for interlace.checks: base classes, all 10 check types,
runner, summary, and the @check decorator.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from interlace.checks.base import (
    CheckResult,
    CheckSeverity,
    CheckStatus,
)
from interlace.checks.decorator import PythonCheck, check, clear_check_registry, get_registered_checks
from interlace.checks.runner import CheckRunner, CheckSummary
from interlace.checks.types.accepted_values import AcceptedValuesCheck
from interlace.checks.types.expression import ExpressionCheck
from interlace.checks.types.freshness import FreshnessCheck
from interlace.checks.types.not_null import NotNullCheck
from interlace.checks.types.pattern import PatternCheck
from interlace.checks.types.range import RangeCheck
from interlace.checks.types.relationships import RelationshipsCheck
from interlace.checks.types.row_count import RowCountCheck
from interlace.checks.types.sql import SqlCheck
from interlace.checks.types.unique import UniqueCheck

# ---------------------------------------------------------------------------
# TestCheckResult
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckResult:
    """Tests for CheckResult dataclass."""

    def test_passed_property_true(self):
        """passed is True when status is PASSED."""
        result = CheckResult(
            check_name="test",
            check_type="unique",
            status=CheckStatus.PASSED,
            severity=CheckSeverity.ERROR,
            table_name="users",
        )
        assert result.passed is True

    def test_passed_property_false_on_failed(self):
        """passed is False when status is FAILED."""
        result = CheckResult(
            check_name="test",
            check_type="unique",
            status=CheckStatus.FAILED,
            severity=CheckSeverity.ERROR,
            table_name="users",
        )
        assert result.passed is False

    def test_passed_property_false_on_error(self):
        """passed is False when status is ERROR."""
        result = CheckResult(
            check_name="test",
            check_type="unique",
            status=CheckStatus.ERROR,
            severity=CheckSeverity.ERROR,
            table_name="users",
        )
        assert result.passed is False

    def test_passed_property_false_on_skipped(self):
        """passed is False when status is SKIPPED."""
        result = CheckResult(
            check_name="test",
            check_type="unique",
            status=CheckStatus.SKIPPED,
            severity=CheckSeverity.ERROR,
            table_name="users",
        )
        assert result.passed is False

    def test_failure_rate_calculation(self):
        """failure_rate returns correct percentage."""
        result = CheckResult(
            check_name="test",
            check_type="unique",
            status=CheckStatus.FAILED,
            severity=CheckSeverity.ERROR,
            table_name="users",
            failed_rows=25,
            total_rows=100,
        )
        assert result.failure_rate == 25.0

    def test_failure_rate_zero_total_rows(self):
        """failure_rate is 0.0 when total_rows is 0."""
        result = CheckResult(
            check_name="test",
            check_type="unique",
            status=CheckStatus.PASSED,
            severity=CheckSeverity.ERROR,
            table_name="users",
            failed_rows=0,
            total_rows=0,
        )
        assert result.failure_rate == 0.0

    def test_failure_rate_all_failed(self):
        """failure_rate is 100.0 when all rows failed."""
        result = CheckResult(
            check_name="test",
            check_type="not_null",
            status=CheckStatus.FAILED,
            severity=CheckSeverity.ERROR,
            table_name="users",
            failed_rows=50,
            total_rows=50,
        )
        assert result.failure_rate == 100.0

    def test_to_dict_serialization(self):
        """to_dict returns correct dictionary."""
        result = CheckResult(
            check_name="unique_user_id",
            check_type="unique",
            status=CheckStatus.PASSED,
            severity=CheckSeverity.ERROR,
            table_name="users",
            column_name="user_id",
            message="All rows unique",
            failed_rows=0,
            total_rows=100,
        )
        d = result.to_dict()
        assert d["check_name"] == "unique_user_id"
        assert d["check_type"] == "unique"
        assert d["status"] == "passed"
        assert d["severity"] == "error"
        assert d["table_name"] == "users"
        assert d["column_name"] == "user_id"
        assert d["message"] == "All rows unique"
        assert d["failed_rows"] == 0
        assert d["total_rows"] == 100
        assert d["failure_rate"] == 0.0
        assert "executed_at" in d
        assert "duration_seconds" in d
        assert "details" in d

    def test_to_dict_includes_failure_rate(self):
        """to_dict includes calculated failure_rate."""
        result = CheckResult(
            check_name="test",
            check_type="not_null",
            status=CheckStatus.FAILED,
            severity=CheckSeverity.WARN,
            table_name="orders",
            failed_rows=10,
            total_rows=200,
        )
        d = result.to_dict()
        assert d["failure_rate"] == 5.0
        assert d["severity"] == "warn"

    def test_defaults(self):
        """Default values are set correctly."""
        result = CheckResult(
            check_name="test",
            check_type="unique",
            status=CheckStatus.PASSED,
            severity=CheckSeverity.ERROR,
            table_name="users",
        )
        assert result.column_name is None
        assert result.message == ""
        assert result.details == {}
        assert result.failed_rows == 0
        assert result.total_rows == 0
        assert result.duration_seconds == 0.0
        assert result.sql_query is None
        assert isinstance(result.executed_at, datetime)


# ---------------------------------------------------------------------------
# TestCheckTypes — constructor validation, check_type, check_name
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckTypes:
    """Tests for check type constructors, check_type, and check_name."""

    # -- UniqueCheck --

    def test_unique_requires_column(self):
        """UniqueCheck raises ValueError without any column."""
        with pytest.raises(ValueError, match="requires at least one column"):
            UniqueCheck()

    def test_unique_check_type(self):
        """UniqueCheck.check_type is 'unique'."""
        assert UniqueCheck(column="id").check_type == "unique"

    def test_unique_check_name_single(self):
        """UniqueCheck auto-generates name from column."""
        assert UniqueCheck(column="user_id").check_name == "unique_user_id"

    def test_unique_check_name_multi(self):
        """UniqueCheck auto-generates name from multiple columns."""
        assert UniqueCheck(columns=["tenant_id", "user_id"]).check_name == "unique_tenant_id_user_id"

    def test_unique_check_name_custom(self):
        """UniqueCheck uses custom name when provided."""
        assert UniqueCheck(column="id", name="pk_unique").check_name == "pk_unique"

    # -- NotNullCheck --

    def test_not_null_requires_column(self):
        """NotNullCheck raises ValueError with None column."""
        with pytest.raises(ValueError, match="requires a column"):
            NotNullCheck(column=None)

    def test_not_null_requires_column_empty(self):
        """NotNullCheck raises ValueError with empty column."""
        with pytest.raises(ValueError, match="requires a column"):
            NotNullCheck(column="")

    def test_not_null_check_type(self):
        """NotNullCheck.check_type is 'not_null'."""
        assert NotNullCheck(column="email").check_type == "not_null"

    def test_not_null_check_name(self):
        """NotNullCheck auto-generates name."""
        assert NotNullCheck(column="email").check_name == "not_null_email"

    # -- AcceptedValuesCheck --

    def test_accepted_values_requires_column(self):
        """AcceptedValuesCheck raises ValueError without column."""
        with pytest.raises(ValueError, match="requires a column"):
            AcceptedValuesCheck(column=None, values=["a"])

    def test_accepted_values_requires_values(self):
        """AcceptedValuesCheck raises ValueError with empty values."""
        with pytest.raises(ValueError, match="requires at least one"):
            AcceptedValuesCheck(column="status", values=[])

    def test_accepted_values_check_type(self):
        """AcceptedValuesCheck.check_type is 'accepted_values'."""
        assert AcceptedValuesCheck(column="status", values=["a", "b"]).check_type == "accepted_values"

    def test_accepted_values_check_name(self):
        """AcceptedValuesCheck auto-generates name."""
        assert AcceptedValuesCheck(column="status", values=["a"]).check_name == "accepted_values_status"

    # -- RowCountCheck --

    def test_row_count_requires_bounds(self):
        """RowCountCheck raises ValueError without min or max."""
        with pytest.raises(ValueError, match="requires min_count"):
            RowCountCheck()

    def test_row_count_check_type(self):
        """RowCountCheck.check_type is 'row_count'."""
        assert RowCountCheck(min_count=1).check_type == "row_count"

    def test_row_count_check_name_min(self):
        """RowCountCheck name contains min."""
        name = RowCountCheck(min_count=100).check_name
        assert "row_count" in name
        assert "min_100" in name

    def test_row_count_check_name_max(self):
        """RowCountCheck name contains max."""
        name = RowCountCheck(max_count=500).check_name
        assert "max_500" in name

    def test_row_count_check_name_both(self):
        """RowCountCheck name contains both bounds."""
        name = RowCountCheck(min_count=10, max_count=1000).check_name
        assert "min_10" in name
        assert "max_1000" in name

    def test_row_count_custom_name(self):
        """RowCountCheck uses custom name."""
        assert RowCountCheck(min_count=1, name="has_rows").check_name == "has_rows"

    # -- FreshnessCheck --

    def test_freshness_requires_column(self):
        """FreshnessCheck raises ValueError without column."""
        with pytest.raises(ValueError, match="requires a column"):
            FreshnessCheck(column=None, max_age_hours=24)

    def test_freshness_requires_max_age(self):
        """FreshnessCheck raises ValueError without any max_age."""
        with pytest.raises(ValueError, match="requires max_age"):
            FreshnessCheck(column="updated_at")

    def test_freshness_check_type(self):
        """FreshnessCheck.check_type is 'freshness'."""
        assert FreshnessCheck(column="ts", max_age_hours=1).check_type == "freshness"

    def test_freshness_check_name(self):
        """FreshnessCheck auto-generates name."""
        assert FreshnessCheck(column="updated_at", max_age_hours=24).check_name == "freshness_updated_at"

    def test_freshness_hours(self):
        """FreshnessCheck stores max_age_hours correctly."""
        assert FreshnessCheck(column="ts", max_age_hours=24).max_age_hours == 24.0

    def test_freshness_days(self):
        """FreshnessCheck converts days to hours."""
        assert FreshnessCheck(column="ts", max_age_days=7).max_age_hours == 168.0

    def test_freshness_minutes(self):
        """FreshnessCheck converts minutes to hours."""
        assert FreshnessCheck(column="ts", max_age_minutes=30).max_age_hours == 0.5

    def test_freshness_combined(self):
        """FreshnessCheck combines all time units."""
        c = FreshnessCheck(column="ts", max_age_days=1, max_age_hours=6, max_age_minutes=30)
        assert c.max_age_hours == 30.5  # 24 + 6 + 0.5

    # -- ExpressionCheck --

    def test_expression_requires_callable(self):
        """ExpressionCheck raises ValueError for non-callable."""
        with pytest.raises(ValueError, match="requires a callable"):
            ExpressionCheck(expression="not callable", name="test")

    def test_expression_requires_name(self):
        """ExpressionCheck raises ValueError for empty name."""
        with pytest.raises(ValueError, match="requires a name"):
            ExpressionCheck(expression=lambda t: t["x"] > 0, name="")

    def test_expression_check_type(self):
        """ExpressionCheck.check_type is 'expression'."""
        assert ExpressionCheck(expression=lambda t: t["x"] > 0, name="pos").check_type == "expression"

    def test_expression_check_name(self):
        """ExpressionCheck uses the provided name."""
        assert ExpressionCheck(expression=lambda t: t["x"] > 0, name="positive_x").check_name == "positive_x"

    # -- RelationshipsCheck --

    def test_relationships_requires_column(self):
        """RelationshipsCheck raises ValueError without column."""
        with pytest.raises(ValueError, match="requires a column"):
            RelationshipsCheck(column="", to_table="customers")

    def test_relationships_requires_to_table(self):
        """RelationshipsCheck raises ValueError without to_table."""
        with pytest.raises(ValueError, match="requires a to_table"):
            RelationshipsCheck(column="customer_id", to_table="")

    def test_relationships_check_type(self):
        """RelationshipsCheck.check_type is 'relationships'."""
        c = RelationshipsCheck(column="customer_id", to_table="customers")
        assert c.check_type == "relationships"

    def test_relationships_check_name_format(self):
        """RelationshipsCheck auto-generates name in correct format."""
        c = RelationshipsCheck(column="customer_id", to_table="customers", to_column="id")
        assert c.check_name == "relationships_customer_id_to_customers_id"

    def test_relationships_check_name_default_to_column(self):
        """RelationshipsCheck defaults to_column to the source column."""
        c = RelationshipsCheck(column="customer_id", to_table="customers")
        assert c.to_column == "customer_id"
        assert c.check_name == "relationships_customer_id_to_customers_customer_id"

    def test_relationships_custom_name(self):
        """RelationshipsCheck uses custom name."""
        c = RelationshipsCheck(column="cid", to_table="customers", name="fk_orders_customers")
        assert c.check_name == "fk_orders_customers"

    # -- PatternCheck --

    def test_pattern_requires_column(self):
        """PatternCheck raises ValueError without column."""
        with pytest.raises(ValueError, match="requires a column"):
            PatternCheck(column="", pattern=r".*")

    def test_pattern_requires_pattern(self):
        """PatternCheck raises ValueError without pattern."""
        with pytest.raises(ValueError, match="requires a pattern"):
            PatternCheck(column="email", pattern="")

    def test_pattern_check_type(self):
        """PatternCheck.check_type is 'pattern'."""
        assert PatternCheck(column="email", pattern=r"@").check_type == "pattern"

    def test_pattern_check_name(self):
        """PatternCheck auto-generates name from column."""
        assert PatternCheck(column="email", pattern=r"@").check_name == "pattern_email"

    # -- RangeCheck --

    def test_range_requires_column(self):
        """RangeCheck raises ValueError without column."""
        with pytest.raises(ValueError, match="requires a column"):
            RangeCheck(column="", min_value=0)

    def test_range_requires_bounds(self):
        """RangeCheck raises ValueError without min or max."""
        with pytest.raises(ValueError, match="requires min_value"):
            RangeCheck(column="price")

    def test_range_check_type(self):
        """RangeCheck.check_type is 'range'."""
        assert RangeCheck(column="price", min_value=0).check_type == "range"

    def test_range_check_name_format(self):
        """RangeCheck auto-generates name in correct format."""
        c = RangeCheck(column="price", min_value=0, max_value=100)
        assert c.check_name == "range_price_min_0_max_100"

    def test_range_check_name_min_only(self):
        """RangeCheck name with min only."""
        c = RangeCheck(column="age", min_value=0)
        assert c.check_name == "range_age_min_0"

    def test_range_check_name_max_only(self):
        """RangeCheck name with max only."""
        c = RangeCheck(column="age", max_value=150)
        assert c.check_name == "range_age_max_150"

    def test_range_custom_name(self):
        """RangeCheck uses custom name."""
        c = RangeCheck(column="x", min_value=0, name="non_negative")
        assert c.check_name == "non_negative"

    def test_range_bounds_str_inclusive(self):
        """RangeCheck._bounds_str with inclusive bounds."""
        c = RangeCheck(column="x", min_value=0, max_value=100, inclusive=True)
        assert c._bounds_str() == "[0, 100]"

    def test_range_bounds_str_exclusive(self):
        """RangeCheck._bounds_str with exclusive bounds."""
        c = RangeCheck(column="x", min_value=0, max_value=100, inclusive=False)
        assert c._bounds_str() == "(0, 100)"

    def test_range_bounds_str_min_only(self):
        """RangeCheck._bounds_str with min only."""
        c = RangeCheck(column="x", min_value=0, inclusive=True)
        assert c._bounds_str() == "[0, inf]"

    def test_range_bounds_str_max_only(self):
        """RangeCheck._bounds_str with max only."""
        c = RangeCheck(column="x", max_value=100, inclusive=True)
        assert c._bounds_str() == "[-inf, 100]"

    # -- SqlCheck --

    def test_sql_requires_sql(self):
        """SqlCheck raises ValueError without sql."""
        with pytest.raises(ValueError, match="requires a sql"):
            SqlCheck(sql="", name="test")

    def test_sql_requires_name(self):
        """SqlCheck raises ValueError without name."""
        with pytest.raises(ValueError, match="requires a name"):
            SqlCheck(sql="SELECT 1", name="")

    def test_sql_check_type(self):
        """SqlCheck.check_type is 'sql'."""
        assert SqlCheck(sql="SELECT 1", name="test").check_type == "sql"

    def test_sql_check_name(self):
        """SqlCheck uses the provided name."""
        assert SqlCheck(sql="SELECT 1", name="my_sql_check").check_name == "my_sql_check"


# ---------------------------------------------------------------------------
# TestCheckSummary
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckSummary:
    """Tests for CheckSummary."""

    def test_has_failures_true_on_error_severity(self):
        """has_failures is True when an ERROR-severity check failed."""
        summary = CheckSummary(table_name="users")
        summary.results.append(
            CheckResult(
                check_name="test",
                check_type="unique",
                status=CheckStatus.FAILED,
                severity=CheckSeverity.ERROR,
                table_name="users",
            )
        )
        assert summary.has_failures is True

    def test_has_failures_false_on_warn_severity(self):
        """has_failures is False when only WARN-severity checks failed."""
        summary = CheckSummary(table_name="users")
        summary.results.append(
            CheckResult(
                check_name="test",
                check_type="unique",
                status=CheckStatus.FAILED,
                severity=CheckSeverity.WARN,
                table_name="users",
            )
        )
        assert summary.has_failures is False

    def test_has_failures_false_on_info_severity(self):
        """has_failures is False when only INFO-severity checks failed."""
        summary = CheckSummary(table_name="users")
        summary.results.append(
            CheckResult(
                check_name="test",
                check_type="not_null",
                status=CheckStatus.FAILED,
                severity=CheckSeverity.INFO,
                table_name="users",
            )
        )
        assert summary.has_failures is False

    def test_has_failures_false_when_passed(self):
        """has_failures is False when all checks passed."""
        summary = CheckSummary(table_name="users")
        summary.results.append(
            CheckResult(
                check_name="test",
                check_type="unique",
                status=CheckStatus.PASSED,
                severity=CheckSeverity.ERROR,
                table_name="users",
            )
        )
        assert summary.has_failures is False

    def test_has_warnings_true(self):
        """has_warnings is True when a WARN-severity check failed."""
        summary = CheckSummary(table_name="users")
        summary.results.append(
            CheckResult(
                check_name="test",
                check_type="unique",
                status=CheckStatus.FAILED,
                severity=CheckSeverity.WARN,
                table_name="users",
            )
        )
        assert summary.has_warnings is True

    def test_has_warnings_false_on_passed(self):
        """has_warnings is False when WARN-severity checks passed."""
        summary = CheckSummary(table_name="users")
        summary.results.append(
            CheckResult(
                check_name="test",
                check_type="unique",
                status=CheckStatus.PASSED,
                severity=CheckSeverity.WARN,
                table_name="users",
            )
        )
        assert summary.has_warnings is False

    def test_has_warnings_false_on_error_severity(self):
        """has_warnings is False when only ERROR-severity checks failed."""
        summary = CheckSummary(table_name="users")
        summary.results.append(
            CheckResult(
                check_name="test",
                check_type="unique",
                status=CheckStatus.FAILED,
                severity=CheckSeverity.ERROR,
                table_name="users",
            )
        )
        assert summary.has_warnings is False

    def test_success_rate(self):
        """success_rate calculates correctly."""
        summary = CheckSummary(table_name="users", total_checks=10, passed=8, failed=2)
        assert summary.success_rate == 80.0

    def test_success_rate_all_passed(self):
        """success_rate is 100% when all passed."""
        summary = CheckSummary(table_name="users", total_checks=5, passed=5)
        assert summary.success_rate == 100.0

    def test_success_rate_none_passed(self):
        """success_rate is 0% when none passed."""
        summary = CheckSummary(table_name="users", total_checks=5, passed=0, failed=5)
        assert summary.success_rate == 0.0

    def test_success_rate_zero_checks(self):
        """success_rate is 100% when no checks."""
        summary = CheckSummary(table_name="users")
        assert summary.success_rate == 100.0

    def test_to_dict(self):
        """to_dict returns complete dictionary."""
        summary = CheckSummary(
            table_name="orders",
            total_checks=3,
            passed=2,
            failed=1,
            errors=0,
            skipped=0,
            duration_seconds=1.5,
        )
        summary.results.append(
            CheckResult(
                check_name="test",
                check_type="unique",
                status=CheckStatus.PASSED,
                severity=CheckSeverity.ERROR,
                table_name="orders",
            )
        )
        d = summary.to_dict()
        assert d["table_name"] == "orders"
        assert d["total_checks"] == 3
        assert d["passed"] == 2
        assert d["failed"] == 1
        assert d["errors"] == 0
        assert d["skipped"] == 0
        assert d["duration_seconds"] == 1.5
        assert isinstance(d["success_rate"], float)
        assert isinstance(d["has_failures"], bool)
        assert isinstance(d["has_warnings"], bool)
        assert isinstance(d["results"], list)
        assert len(d["results"]) == 1

    def test_to_dict_empty(self):
        """to_dict works with no results."""
        summary = CheckSummary(table_name="empty")
        d = summary.to_dict()
        assert d["results"] == []
        assert d["success_rate"] == 100.0


# ---------------------------------------------------------------------------
# TestCheckRunner
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckRunner:
    """Tests for CheckRunner."""

    def test_run_checks_raises_without_connection(self):
        """run_checks raises ValueError when no connection provided."""
        runner = CheckRunner()
        with pytest.raises(ValueError, match="No connection"):
            runner.run_checks("users", [])

    def test_run_checks_uses_override_connection(self):
        """run_checks accepts a connection override argument."""
        runner = CheckRunner()
        conn = MagicMock()
        table = MagicMock()
        conn.table.return_value = table
        table.count.return_value.execute.return_value = 10
        table.__getitem__ = MagicMock(return_value=MagicMock())
        table.__getitem__.return_value.nunique.return_value.execute.return_value = 10

        summary = runner.run_checks("users", [UniqueCheck(column="id")], connection=conn)
        assert summary.total_checks == 1
        assert summary.passed == 1

    def test_parse_check_configs_all_types(self):
        """_parse_check_configs creates correct types for all 10 check types."""
        runner = CheckRunner()
        configs = [
            {"type": "unique", "column": "id"},
            {"type": "not_null", "column": "email"},
            {"type": "accepted_values", "column": "status", "values": ["a", "b"]},
            {"type": "freshness", "column": "ts", "max_age_hours": 24},
            {"type": "row_count", "min_count": 1},
            {"type": "expression", "expression": lambda t: t["x"] > 0, "name": "pos_x"},
            {"type": "relationships", "column": "cid", "to_table": "customers"},
            {"type": "pattern", "column": "email", "pattern": r"@"},
            {"type": "range", "column": "price", "min_value": 0},
            {"type": "sql", "sql": "SELECT 1", "name": "sql_test"},
        ]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 10
        assert isinstance(checks[0], UniqueCheck)
        assert isinstance(checks[1], NotNullCheck)
        assert isinstance(checks[2], AcceptedValuesCheck)
        assert isinstance(checks[3], FreshnessCheck)
        assert isinstance(checks[4], RowCountCheck)
        assert isinstance(checks[5], ExpressionCheck)
        assert isinstance(checks[6], RelationshipsCheck)
        assert isinstance(checks[7], PatternCheck)
        assert isinstance(checks[8], RangeCheck)
        assert isinstance(checks[9], SqlCheck)

    def test_parse_check_configs_unknown_type_skipped(self):
        """Unknown check type is skipped."""
        runner = CheckRunner()
        configs = [{"type": "unknown_check", "column": "x"}]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 0

    def test_parse_check_configs_missing_type_skipped(self):
        """Config without type is skipped."""
        runner = CheckRunner()
        configs = [{"column": "x"}]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 0

    def test_parse_check_configs_invalid_params_skipped(self):
        """Config with invalid params is skipped (logged, not raised)."""
        runner = CheckRunner()
        # NotNullCheck requires a non-empty column
        configs = [{"type": "not_null", "column": ""}]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 0

    def test_parse_severity_error(self):
        """Severity 'error' maps to CheckSeverity.ERROR."""
        runner = CheckRunner()
        checks = runner._parse_check_configs([{"type": "not_null", "column": "x", "severity": "error"}])
        assert checks[0].severity == CheckSeverity.ERROR

    def test_parse_severity_warn(self):
        """Severity 'warn' maps to CheckSeverity.WARN."""
        runner = CheckRunner()
        checks = runner._parse_check_configs([{"type": "not_null", "column": "x", "severity": "warn"}])
        assert checks[0].severity == CheckSeverity.WARN

    def test_parse_severity_warning(self):
        """Severity 'warning' maps to CheckSeverity.WARN."""
        runner = CheckRunner()
        checks = runner._parse_check_configs([{"type": "not_null", "column": "x", "severity": "warning"}])
        assert checks[0].severity == CheckSeverity.WARN

    def test_parse_severity_info(self):
        """Severity 'info' maps to CheckSeverity.INFO."""
        runner = CheckRunner()
        checks = runner._parse_check_configs([{"type": "not_null", "column": "x", "severity": "info"}])
        assert checks[0].severity == CheckSeverity.INFO

    def test_parse_severity_default(self):
        """Default severity is ERROR."""
        runner = CheckRunner()
        checks = runner._parse_check_configs([{"type": "not_null", "column": "x"}])
        assert checks[0].severity == CheckSeverity.ERROR

    def test_parse_severity_unknown_falls_back_to_error(self):
        """Unknown severity string falls back to ERROR."""
        runner = CheckRunner()
        checks = runner._parse_check_configs([{"type": "not_null", "column": "x", "severity": "critical"}])
        assert checks[0].severity == CheckSeverity.ERROR

    def test_run_model_checks_no_checks(self):
        """run_model_checks returns None when no checks defined."""
        runner = CheckRunner(connection=MagicMock())
        result = runner.run_model_checks("my_model", {})
        assert result is None

    def test_run_model_checks_empty_list(self):
        """run_model_checks returns None for empty checks list."""
        runner = CheckRunner(connection=MagicMock())
        result = runner.run_model_checks("my_model", {"checks": []})
        assert result is None

    def test_run_model_checks_with_check_instances(self):
        """run_model_checks accepts Check instances directly."""
        conn = MagicMock()
        table = MagicMock()
        conn.table.return_value = table
        table.count.return_value.execute.return_value = 10
        table.filter.return_value.count.return_value.execute.return_value = 0

        runner = CheckRunner(connection=conn)
        model_info = {"checks": [NotNullCheck(column="email")]}
        summary = runner.run_model_checks("my_model", model_info)
        assert summary is not None
        assert summary.passed == 1

    def test_run_model_checks_with_config_dicts(self):
        """run_model_checks parses config dicts."""
        conn = MagicMock()
        table = MagicMock()
        conn.table.return_value = table
        table.count.return_value.execute.return_value = 10
        table.filter.return_value.count.return_value.execute.return_value = 0

        runner = CheckRunner(connection=conn)
        model_info = {"checks": [{"type": "not_null", "column": "email"}]}
        summary = runner.run_model_checks("my_model", model_info)
        assert summary is not None
        assert summary.passed == 1

    def test_fail_fast_stops_after_error_failure(self):
        """fail_fast=True stops execution after first ERROR-severity failure."""
        conn = MagicMock()
        table = MagicMock()
        conn.table.return_value = table
        table.count.return_value.execute.return_value = 100
        # Both checks will fail (5 nulls)
        table.filter.return_value.count.return_value.execute.return_value = 5

        runner = CheckRunner(connection=conn, fail_fast=True)
        checks = [
            NotNullCheck(column="col1", severity=CheckSeverity.ERROR),
            NotNullCheck(column="col2", severity=CheckSeverity.ERROR),
        ]
        summary = runner.run_checks("users", checks)
        # Should stop after first failure
        assert summary.failed == 1
        assert len(summary.results) == 1

    def test_fail_fast_continues_on_warn(self):
        """fail_fast=True does not stop on WARN-severity failure."""
        conn = MagicMock()
        table = MagicMock()
        conn.table.return_value = table
        table.count.return_value.execute.return_value = 100
        table.filter.return_value.count.return_value.execute.return_value = 5

        runner = CheckRunner(connection=conn, fail_fast=True)
        checks = [
            NotNullCheck(column="col1", severity=CheckSeverity.WARN),
            NotNullCheck(column="col2", severity=CheckSeverity.WARN),
        ]
        summary = runner.run_checks("users", checks)
        # Should continue past WARN failures
        assert summary.failed == 2
        assert len(summary.results) == 2

    def test_check_exception_counted_as_error(self):
        """A check that raises an exception is counted as an error."""
        conn = MagicMock()
        conn.table.side_effect = RuntimeError("connection lost")

        runner = CheckRunner(connection=conn)
        checks = [NotNullCheck(column="email")]
        summary = runner.run_checks("users", checks)
        # The check.run catches the exception internally and returns ERROR status
        assert summary.errors == 1
        assert summary.results[0].status == CheckStatus.ERROR


# ---------------------------------------------------------------------------
# TestCheckRunnerWithMockedConnection
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckRunnerWithMockedConnection:
    """Tests that run each check type with a mocked ibis connection."""

    @pytest.fixture
    def mock_conn(self):
        """Create a mock ibis connection with table mock."""
        conn = MagicMock()
        table = MagicMock()
        conn.table.return_value = table
        # Default: 10 rows
        table.count.return_value.execute.return_value = 10
        return conn, table

    # -- NotNullCheck --

    def test_not_null_passes(self, mock_conn):
        """NotNullCheck passes when no nulls found."""
        conn, table = mock_conn
        table.filter.return_value.count.return_value.execute.return_value = 0

        result = NotNullCheck(column="email").run(conn, "users")
        assert result.passed
        assert result.status == CheckStatus.PASSED
        assert result.failed_rows == 0
        assert result.total_rows == 10

    def test_not_null_fails(self, mock_conn):
        """NotNullCheck fails when nulls found."""
        conn, table = mock_conn
        table.filter.return_value.count.return_value.execute.return_value = 3

        result = NotNullCheck(column="email").run(conn, "users")
        assert not result.passed
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows == 3
        assert result.total_rows == 10

    def test_not_null_error(self):
        """NotNullCheck returns ERROR status on exception."""
        conn = MagicMock()
        conn.table.side_effect = RuntimeError("boom")

        result = NotNullCheck(column="email").run(conn, "users")
        assert result.status == CheckStatus.ERROR
        assert "boom" in result.message

    # -- UniqueCheck --

    def test_unique_passes(self, mock_conn):
        """UniqueCheck passes when all values are unique."""
        conn, table = mock_conn
        table.__getitem__ = MagicMock(return_value=MagicMock())
        table.__getitem__.return_value.nunique.return_value.execute.return_value = 10

        result = UniqueCheck(column="id").run(conn, "users")
        assert result.passed
        assert result.failed_rows == 0

    def test_unique_fails(self, mock_conn):
        """UniqueCheck fails when duplicates exist."""
        conn, table = mock_conn
        table.__getitem__ = MagicMock(return_value=MagicMock())
        table.__getitem__.return_value.nunique.return_value.execute.return_value = 7

        result = UniqueCheck(column="id").run(conn, "users")
        assert not result.passed
        assert result.failed_rows == 3  # 10 - 7

    def test_unique_multi_column(self, mock_conn):
        """UniqueCheck with multiple columns uses distinct().count()."""
        conn, table = mock_conn
        table.select.return_value.distinct.return_value.count.return_value.execute.return_value = 10

        result = UniqueCheck(columns=["tenant", "id"]).run(conn, "users")
        assert result.passed

    def test_unique_multi_column_fails(self, mock_conn):
        """UniqueCheck multi-column fails when duplicates exist."""
        conn, table = mock_conn
        table.select.return_value.distinct.return_value.count.return_value.execute.return_value = 8

        result = UniqueCheck(columns=["tenant", "id"]).run(conn, "users")
        assert not result.passed
        assert result.failed_rows == 2

    def test_unique_error(self):
        """UniqueCheck returns ERROR on exception."""
        conn = MagicMock()
        conn.table.side_effect = RuntimeError("fail")

        result = UniqueCheck(column="id").run(conn, "users")
        assert result.status == CheckStatus.ERROR

    # -- AcceptedValuesCheck --

    def test_accepted_values_passes(self, mock_conn):
        """AcceptedValuesCheck passes when all values are accepted."""
        conn, table = mock_conn
        table.__getitem__ = MagicMock(return_value=MagicMock())
        table.filter.return_value.count.return_value.execute.return_value = 0

        result = AcceptedValuesCheck(column="status", values=["active", "inactive"]).run(conn, "users")
        assert result.passed
        assert result.failed_rows == 0

    def test_accepted_values_fails(self, mock_conn):
        """AcceptedValuesCheck fails when invalid values found."""
        conn, table = mock_conn
        table.__getitem__ = MagicMock(return_value=MagicMock())
        table.filter.return_value.count.return_value.execute.return_value = 2
        # Mock the sample query for invalid values
        table.filter.return_value.select.return_value.distinct.return_value.limit.return_value.execute.return_value = (
            MagicMock()
        )

        result = AcceptedValuesCheck(column="status", values=["active"]).run(conn, "users")
        assert not result.passed
        assert result.failed_rows == 2

    def test_accepted_values_error(self):
        """AcceptedValuesCheck returns ERROR on exception."""
        conn = MagicMock()
        conn.table.side_effect = RuntimeError("fail")

        result = AcceptedValuesCheck(column="status", values=["a"]).run(conn, "users")
        assert result.status == CheckStatus.ERROR

    # -- RowCountCheck --

    def test_row_count_passes_within_bounds(self, mock_conn):
        """RowCountCheck passes when row count is within bounds."""
        conn, table = mock_conn
        table.count.return_value.execute.return_value = 500

        result = RowCountCheck(min_count=100, max_count=1000).run(conn, "users")
        assert result.passed

    def test_row_count_passes_min_only(self, mock_conn):
        """RowCountCheck passes when row count >= min."""
        conn, table = mock_conn
        table.count.return_value.execute.return_value = 100

        result = RowCountCheck(min_count=100).run(conn, "users")
        assert result.passed

    def test_row_count_passes_max_only(self, mock_conn):
        """RowCountCheck passes when row count <= max."""
        conn, table = mock_conn
        table.count.return_value.execute.return_value = 500

        result = RowCountCheck(max_count=1000).run(conn, "users")
        assert result.passed

    def test_row_count_fails_below_min(self, mock_conn):
        """RowCountCheck fails when row count < min."""
        conn, table = mock_conn
        table.count.return_value.execute.return_value = 50

        result = RowCountCheck(min_count=100).run(conn, "users")
        assert not result.passed
        assert "below minimum" in result.message

    def test_row_count_fails_above_max(self, mock_conn):
        """RowCountCheck fails when row count > max."""
        conn, table = mock_conn
        table.count.return_value.execute.return_value = 2000

        result = RowCountCheck(max_count=1000).run(conn, "users")
        assert not result.passed
        assert "exceeds maximum" in result.message

    def test_row_count_error(self):
        """RowCountCheck returns ERROR on exception."""
        conn = MagicMock()
        conn.table.side_effect = RuntimeError("fail")

        result = RowCountCheck(min_count=1).run(conn, "users")
        assert result.status == CheckStatus.ERROR

    # -- FreshnessCheck --

    def test_freshness_passes(self, mock_conn):
        """FreshnessCheck passes when data is recent."""
        conn, table = mock_conn
        table.__getitem__ = MagicMock(return_value=MagicMock())
        # Max timestamp is 1 hour ago
        recent = datetime.now() - timedelta(hours=1)
        table.__getitem__.return_value.max.return_value.execute.return_value = recent

        result = FreshnessCheck(column="updated_at", max_age_hours=24).run(conn, "users")
        assert result.passed

    def test_freshness_fails(self, mock_conn):
        """FreshnessCheck fails when data is stale."""
        conn, table = mock_conn
        table.__getitem__ = MagicMock(return_value=MagicMock())
        # Max timestamp is 48 hours ago
        stale = datetime.now() - timedelta(hours=48)
        table.__getitem__.return_value.max.return_value.execute.return_value = stale

        result = FreshnessCheck(column="updated_at", max_age_hours=24).run(conn, "users")
        assert not result.passed
        assert "stale" in result.message.lower()

    def test_freshness_skipped_empty_table(self, mock_conn):
        """FreshnessCheck is SKIPPED on empty table."""
        conn, table = mock_conn
        table.count.return_value.execute.return_value = 0

        result = FreshnessCheck(column="updated_at", max_age_hours=24).run(conn, "users")
        assert result.status == CheckStatus.SKIPPED

    def test_freshness_fails_all_null(self, mock_conn):
        """FreshnessCheck fails when max timestamp is NULL."""
        conn, table = mock_conn
        table.__getitem__ = MagicMock(return_value=MagicMock())
        table.__getitem__.return_value.max.return_value.execute.return_value = None

        result = FreshnessCheck(column="updated_at", max_age_hours=24).run(conn, "users")
        assert not result.passed
        assert "no non-NULL" in result.message

    def test_freshness_error(self):
        """FreshnessCheck returns ERROR on exception."""
        conn = MagicMock()
        conn.table.side_effect = RuntimeError("fail")

        result = FreshnessCheck(column="ts", max_age_hours=1).run(conn, "users")
        assert result.status == CheckStatus.ERROR

    # -- ExpressionCheck --

    def test_expression_passes(self, mock_conn):
        """ExpressionCheck passes when no rows fail the expression."""
        conn, table = mock_conn
        expr_result = MagicMock()
        # The expression returns a condition; ~condition filters failing rows
        table.filter.return_value.count.return_value.execute.return_value = 0

        result = ExpressionCheck(expression=lambda t: expr_result, name="positive_x").run(conn, "users")
        assert result.passed
        assert result.failed_rows == 0

    def test_expression_fails(self, mock_conn):
        """ExpressionCheck fails when some rows fail the expression."""
        conn, table = mock_conn
        expr_result = MagicMock()
        table.filter.return_value.count.return_value.execute.return_value = 4

        result = ExpressionCheck(expression=lambda t: expr_result, name="positive_x").run(conn, "users")
        assert not result.passed
        assert result.failed_rows == 4

    def test_expression_skipped_empty_table(self, mock_conn):
        """ExpressionCheck is SKIPPED on empty table."""
        conn, table = mock_conn
        table.count.return_value.execute.return_value = 0

        result = ExpressionCheck(expression=lambda t: MagicMock(), name="test").run(conn, "users")
        assert result.status == CheckStatus.SKIPPED

    def test_expression_error(self):
        """ExpressionCheck returns ERROR on exception."""
        conn = MagicMock()
        conn.table.side_effect = RuntimeError("fail")

        result = ExpressionCheck(expression=lambda t: t["x"] > 0, name="pos").run(conn, "users")
        assert result.status == CheckStatus.ERROR

    # -- RelationshipsCheck --

    def test_relationships_passes(self, mock_conn):
        """RelationshipsCheck passes when all child values exist in parent."""
        conn, table = mock_conn
        parent_table = MagicMock()
        # conn.table called twice: child then parent
        conn.table.side_effect = [table, parent_table]
        table.__getitem__ = MagicMock(return_value=MagicMock())
        parent_table.select.return_value.distinct.return_value = MagicMock()
        parent_table.select.return_value.distinct.return_value.__getitem__ = MagicMock(return_value=MagicMock())
        table.filter.return_value.count.return_value.execute.return_value = 0

        result = RelationshipsCheck(column="cid", to_table="customers", to_column="id").run(conn, "orders")
        assert result.passed
        assert result.failed_rows == 0

    def test_relationships_fails(self, mock_conn):
        """RelationshipsCheck fails when orphaned rows exist."""
        conn, table = mock_conn
        parent_table = MagicMock()
        conn.table.side_effect = [table, parent_table]
        table.__getitem__ = MagicMock(return_value=MagicMock())
        parent_table.select.return_value.distinct.return_value = MagicMock()
        parent_table.select.return_value.distinct.return_value.__getitem__ = MagicMock(return_value=MagicMock())
        table.filter.return_value.count.return_value.execute.return_value = 3
        # Mock sample query
        table.filter.return_value.select.return_value.distinct.return_value.limit.return_value.execute.return_value = (
            MagicMock()
        )

        result = RelationshipsCheck(column="cid", to_table="customers", to_column="id").run(conn, "orders")
        assert not result.passed
        assert result.failed_rows == 3

    def test_relationships_skipped_empty_table(self, mock_conn):
        """RelationshipsCheck is SKIPPED on empty child table."""
        conn, table = mock_conn
        parent_table = MagicMock()
        conn.table.side_effect = [table, parent_table]
        table.count.return_value.execute.return_value = 0

        result = RelationshipsCheck(column="cid", to_table="customers").run(conn, "orders")
        assert result.status == CheckStatus.SKIPPED

    def test_relationships_error(self):
        """RelationshipsCheck returns ERROR on exception."""
        conn = MagicMock()
        conn.table.side_effect = RuntimeError("fail")

        result = RelationshipsCheck(column="cid", to_table="customers").run(conn, "orders")
        assert result.status == CheckStatus.ERROR

    # -- PatternCheck --

    def test_pattern_passes(self, mock_conn):
        """PatternCheck passes when all values match pattern."""
        conn, table = mock_conn
        table.__getitem__ = MagicMock(return_value=MagicMock())
        table.filter.return_value.count.return_value.execute.return_value = 0

        result = PatternCheck(column="email", pattern=r"@").run(conn, "users")
        assert result.passed
        assert result.failed_rows == 0

    def test_pattern_fails(self, mock_conn):
        """PatternCheck fails when some values don't match pattern."""
        conn, table = mock_conn
        table.__getitem__ = MagicMock(return_value=MagicMock())
        table.filter.return_value.count.return_value.execute.return_value = 2
        # Mock sample query
        table.filter.return_value.select.return_value.limit.return_value.execute.return_value = MagicMock()

        result = PatternCheck(column="email", pattern=r"^[^@]+@[^@]+$").run(conn, "users")
        assert not result.passed
        assert result.failed_rows == 2

    def test_pattern_skipped_empty_table(self, mock_conn):
        """PatternCheck is SKIPPED on empty table."""
        conn, table = mock_conn
        table.count.return_value.execute.return_value = 0

        result = PatternCheck(column="email", pattern=r"@").run(conn, "users")
        assert result.status == CheckStatus.SKIPPED

    def test_pattern_error(self):
        """PatternCheck returns ERROR on exception."""
        conn = MagicMock()
        conn.table.side_effect = RuntimeError("fail")

        result = PatternCheck(column="email", pattern=r"@").run(conn, "users")
        assert result.status == CheckStatus.ERROR

    # -- RangeCheck --

    def _setup_range_mock(self, mock_conn):
        """Set up mocks for RangeCheck which needs comparison operators on column."""
        conn, table = mock_conn
        col_mock = MagicMock()
        # RangeCheck does col < value, col > value, col.isnull(), and OR-combines conditions.
        # MagicMock doesn't support __lt__/__gt__ by default so we configure them.
        null_cond = MagicMock()
        lt_cond = MagicMock()
        gt_cond = MagicMock()
        le_cond = MagicMock()
        ge_cond = MagicMock()
        col_mock.isnull.return_value = null_cond
        col_mock.__lt__ = MagicMock(return_value=lt_cond)
        col_mock.__gt__ = MagicMock(return_value=gt_cond)
        col_mock.__le__ = MagicMock(return_value=le_cond)
        col_mock.__ge__ = MagicMock(return_value=ge_cond)
        # OR combination: null_cond | lt_cond etc. — need __or__ on each condition
        combined = MagicMock()
        null_cond.__or__ = MagicMock(return_value=combined)
        combined.__or__ = MagicMock(return_value=combined)
        table.__getitem__ = MagicMock(return_value=col_mock)
        return conn, table

    def test_range_passes(self, mock_conn):
        """RangeCheck passes when all values are within range."""
        conn, table = self._setup_range_mock(mock_conn)
        table.filter.return_value.count.return_value.execute.return_value = 0

        result = RangeCheck(column="price", min_value=0, max_value=1000).run(conn, "products")
        assert result.passed
        assert result.failed_rows == 0

    def test_range_fails(self, mock_conn):
        """RangeCheck fails when some values are out of range."""
        conn, table = self._setup_range_mock(mock_conn)
        table.filter.return_value.count.return_value.execute.return_value = 4

        result = RangeCheck(column="price", min_value=0, max_value=1000).run(conn, "products")
        assert not result.passed
        assert result.failed_rows == 4

    def test_range_skipped_empty_table(self, mock_conn):
        """RangeCheck is SKIPPED on empty table."""
        conn, table = mock_conn
        table.count.return_value.execute.return_value = 0

        result = RangeCheck(column="price", min_value=0).run(conn, "products")
        assert result.status == CheckStatus.SKIPPED

    def test_range_min_only(self, mock_conn):
        """RangeCheck with only min_value passes correctly."""
        conn, table = self._setup_range_mock(mock_conn)
        table.filter.return_value.count.return_value.execute.return_value = 0

        result = RangeCheck(column="age", min_value=0).run(conn, "users")
        assert result.passed

    def test_range_max_only(self, mock_conn):
        """RangeCheck with only max_value passes correctly."""
        conn, table = self._setup_range_mock(mock_conn)
        table.filter.return_value.count.return_value.execute.return_value = 0

        result = RangeCheck(column="age", max_value=150).run(conn, "users")
        assert result.passed

    def test_range_error(self):
        """RangeCheck returns ERROR on exception."""
        conn = MagicMock()
        conn.table.side_effect = RuntimeError("fail")

        result = RangeCheck(column="price", min_value=0).run(conn, "products")
        assert result.status == CheckStatus.ERROR

    # -- SqlCheck --

    def test_sql_passes(self, mock_conn):
        """SqlCheck passes when query returns 0 rows."""
        conn, _ = mock_conn
        result_table = MagicMock()
        conn.sql.return_value = result_table
        result_table.count.return_value.execute.return_value = 0

        result = SqlCheck(sql="SELECT * FROM {table} WHERE total < 0", name="no_neg").run(conn, "orders")
        assert result.passed
        assert result.failed_rows == 0
        # Verify placeholder substitution
        conn.sql.assert_called_once_with("SELECT * FROM orders WHERE total < 0")

    def test_sql_fails(self, mock_conn):
        """SqlCheck fails when query returns rows."""
        conn, _ = mock_conn
        result_table = MagicMock()
        conn.sql.return_value = result_table
        result_table.count.return_value.execute.return_value = 5

        result = SqlCheck(sql="SELECT * FROM {table} WHERE total < 0", name="no_neg").run(conn, "orders")
        assert not result.passed
        assert result.failed_rows == 5

    def test_sql_schema_placeholder(self, mock_conn):
        """SqlCheck substitutes {schema} placeholder."""
        conn, _ = mock_conn
        result_table = MagicMock()
        conn.sql.return_value = result_table
        result_table.count.return_value.execute.return_value = 0

        SqlCheck(sql="SELECT * FROM {schema}.{table}", name="test").run(conn, "orders", schema="analytics")
        conn.sql.assert_called_once_with("SELECT * FROM analytics.orders")

    def test_sql_schema_defaults_to_public(self, mock_conn):
        """SqlCheck defaults {schema} to 'public' when not provided."""
        conn, _ = mock_conn
        result_table = MagicMock()
        conn.sql.return_value = result_table
        result_table.count.return_value.execute.return_value = 0

        SqlCheck(sql="SELECT * FROM {schema}.{table}", name="test").run(conn, "orders")
        conn.sql.assert_called_once_with("SELECT * FROM public.orders")

    def test_sql_error(self):
        """SqlCheck returns ERROR on exception."""
        conn = MagicMock()
        conn.sql.side_effect = RuntimeError("syntax error")

        result = SqlCheck(sql="INVALID SQL", name="bad").run(conn, "orders")
        assert result.status == CheckStatus.ERROR
        assert "syntax error" in result.message

    def test_sql_stores_query(self, mock_conn):
        """SqlCheck stores the resolved SQL in the result."""
        conn, _ = mock_conn
        result_table = MagicMock()
        conn.sql.return_value = result_table
        result_table.count.return_value.execute.return_value = 0

        result = SqlCheck(sql="SELECT * FROM {table}", name="test").run(conn, "orders")
        assert result.sql_query == "SELECT * FROM orders"


# ---------------------------------------------------------------------------
# TestNewCheckTypes — focused tests for RelationshipsCheck, PatternCheck,
# RangeCheck, SqlCheck specifics
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNewCheckTypes:
    """Focused tests for the 3 new check types + SqlCheck specifics."""

    # -- RelationshipsCheck specifics --

    def test_relationships_to_column_defaults_to_column(self):
        """RelationshipsCheck defaults to_column to source column."""
        c = RelationshipsCheck(column="customer_id", to_table="customers")
        assert c.to_column == "customer_id"

    def test_relationships_to_schema_stored(self):
        """RelationshipsCheck stores to_schema."""
        c = RelationshipsCheck(column="cid", to_table="customers", to_schema="public")
        assert c.to_schema == "public"

    def test_relationships_uses_to_schema(self):
        """RelationshipsCheck passes to_schema when getting parent table."""
        conn = MagicMock()
        child_table = MagicMock()
        parent_table = MagicMock()
        conn.table.side_effect = [child_table, parent_table]
        child_table.count.return_value.execute.return_value = 5
        child_table.__getitem__ = MagicMock(return_value=MagicMock())
        parent_table.select.return_value.distinct.return_value = MagicMock()
        parent_table.select.return_value.distinct.return_value.__getitem__ = MagicMock(return_value=MagicMock())
        child_table.filter.return_value.count.return_value.execute.return_value = 0

        RelationshipsCheck(column="cid", to_table="customers", to_schema="analytics").run(conn, "orders")
        # Second call should use schema
        calls = conn.table.call_args_list
        assert calls[1] == (("customers",), {"database": "analytics"})

    # -- PatternCheck specifics --

    def test_pattern_stores_pattern(self):
        """PatternCheck stores the pattern string."""
        c = PatternCheck(column="email", pattern=r"^.+@.+$")
        assert c.pattern == r"^.+@.+$"

    def test_pattern_custom_name(self):
        """PatternCheck uses custom name."""
        c = PatternCheck(column="email", pattern=r"@", name="valid_email")
        assert c.check_name == "valid_email"

    # -- RangeCheck specifics --

    def test_range_stores_inclusive_flag(self):
        """RangeCheck stores inclusive parameter."""
        c = RangeCheck(column="x", min_value=0, inclusive=False)
        assert c.inclusive is False

    def test_range_default_inclusive(self):
        """RangeCheck defaults inclusive to True."""
        c = RangeCheck(column="x", min_value=0)
        assert c.inclusive is True

    def test_range_bounds_str_partial_min(self):
        """RangeCheck._bounds_str with min only shows inf for max."""
        c = RangeCheck(column="x", min_value=5, inclusive=True)
        assert c._bounds_str() == "[5, inf]"

    def test_range_bounds_str_partial_max(self):
        """RangeCheck._bounds_str with max only shows -inf for min."""
        c = RangeCheck(column="x", max_value=10, inclusive=True)
        assert c._bounds_str() == "[-inf, 10]"

    def test_range_bounds_str_exclusive(self):
        """RangeCheck._bounds_str uses parentheses for exclusive."""
        c = RangeCheck(column="x", min_value=0, max_value=100, inclusive=False)
        assert c._bounds_str() == "(0, 100)"

    # -- SqlCheck specifics --

    def test_sql_stores_query(self):
        """SqlCheck stores the sql string."""
        c = SqlCheck(sql="SELECT * FROM orders WHERE total < 0", name="no_neg")
        assert c.sql == "SELECT * FROM orders WHERE total < 0"

    def test_sql_check_name_is_name(self):
        """SqlCheck.check_name is the provided name."""
        c = SqlCheck(sql="SELECT 1", name="my_check")
        assert c.check_name == "my_check"


# ---------------------------------------------------------------------------
# TestCheckDecorator — @check decorator and PythonCheck
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckDecorator:
    """Tests for the @check decorator and PythonCheck class."""

    def setup_method(self):
        """Clear registry before each test."""
        clear_check_registry()

    def teardown_method(self):
        """Clear registry after each test."""
        clear_check_registry()

    def test_decorator_registers_function(self):
        """@check decorator registers the function in the global registry."""

        @check(name="no_nulls", model="users")
        def my_check(table, connection):
            return True

        registered = get_registered_checks()
        assert len(registered) == 1
        assert registered[0]["name"] == "no_nulls"
        assert registered[0]["model"] == ["users"]
        assert registered[0]["function"] is my_check

    def test_decorator_multiple_models(self):
        """@check with list of models stores all."""

        @check(name="test", model=["users", "orders"])
        def my_check(table, connection):
            return True

        registered = get_registered_checks()
        assert registered[0]["model"] == ["users", "orders"]

    def test_decorator_severity(self):
        """@check stores severity."""

        @check(name="test", model="users", severity="warn")
        def my_check(table, connection):
            return True

        assert get_registered_checks()[0]["severity"] == "warn"

    def test_decorator_description(self):
        """@check stores description."""

        @check(name="test", model="users", description="My desc")
        def my_check(table, connection):
            return True

        assert get_registered_checks()[0]["description"] == "My desc"

    def test_decorator_uses_docstring(self):
        """@check uses function docstring when description not provided."""

        @check(name="test", model="users")
        def my_check(table, connection):
            """Check for something."""
            return True

        assert get_registered_checks()[0]["description"] == "Check for something."

    def test_decorator_sets_attribute(self):
        """@check sets _interlace_check attribute on function."""

        @check(name="test", model="users")
        def my_check(table, connection):
            return True

        assert hasattr(my_check, "_interlace_check")
        assert my_check._interlace_check["name"] == "test"

    def test_python_check_type(self):
        """PythonCheck.check_type is 'python'."""

        def my_func(table, conn):
            return True

        pc = PythonCheck(func=my_func, name="test")
        assert pc.check_type == "python"

    def test_python_check_name(self):
        """PythonCheck uses provided name."""

        def my_func(table, conn):
            return True

        pc = PythonCheck(func=my_func, name="my_test")
        assert pc.check_name == "my_test"

    def test_python_check_bool_true_passes(self):
        """PythonCheck passes when function returns True."""
        conn = MagicMock()
        conn.table.return_value = MagicMock()

        pc = PythonCheck(func=lambda t, c: True, name="test")
        result = pc.run(conn, "users")
        assert result.passed

    def test_python_check_bool_false_fails(self):
        """PythonCheck fails when function returns False."""
        conn = MagicMock()
        conn.table.return_value = MagicMock()

        pc = PythonCheck(func=lambda t, c: False, name="test")
        result = pc.run(conn, "users")
        assert not result.passed

    def test_python_check_int_zero_passes(self):
        """PythonCheck passes when function returns 0."""
        conn = MagicMock()
        conn.table.return_value = MagicMock()

        pc = PythonCheck(func=lambda t, c: 0, name="test")
        result = pc.run(conn, "users")
        assert result.passed

    def test_python_check_int_nonzero_fails(self):
        """PythonCheck fails when function returns nonzero int."""
        conn = MagicMock()
        conn.table.return_value = MagicMock()

        pc = PythonCheck(func=lambda t, c: 5, name="test")
        result = pc.run(conn, "users")
        assert not result.passed
        assert result.failed_rows == 5

    def test_python_check_error_on_exception(self):
        """PythonCheck returns ERROR when function raises."""
        conn = MagicMock()
        conn.table.return_value = MagicMock()

        def bad_func(t, c):
            raise ValueError("bad data")

        pc = PythonCheck(func=bad_func, name="test")
        result = pc.run(conn, "users")
        assert result.status == CheckStatus.ERROR
        assert "bad data" in result.message

    def test_python_check_severity(self):
        """PythonCheck respects custom severity."""

        def my_func(t, c):
            return True

        pc = PythonCheck(func=my_func, name="test", severity=CheckSeverity.WARN)
        assert pc.severity == CheckSeverity.WARN

    def test_clear_registry(self):
        """clear_check_registry removes all registered checks."""

        @check(name="test", model="users")
        def my_check(table, connection):
            return True

        assert len(get_registered_checks()) == 1
        clear_check_registry()
        assert len(get_registered_checks()) == 0
