"""
Integration tests for data checks against a real DuckDB database.

Tests all check types with both PASS and FAIL scenarios, plus CheckRunner
end-to-end orchestration and config parsing.
"""

from typing import Any

import ibis
import pytest

from interlace.checks import (
    Check,
    CheckRunner,
    CheckStatus,
    CheckSummary,
)
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


@pytest.fixture
def duckdb_conn() -> ibis.BaseBackend:
    """Create an in-memory DuckDB connection with test data.

    Tables:
        users: 5 rows with a NULL email (row 2), negative age (row 3),
               'unknown' status (row 4), and NULL created_at (row 5).
               Row 4 has a created_at that is 48 hours old.
        orders: 4 rows with an orphaned user_id=999 (row 3) and
                a negative total (row 3).
    """
    conn = ibis.duckdb.connect()
    conn.raw_sql("""
        CREATE TABLE users (
            id INTEGER,
            email VARCHAR,
            status VARCHAR,
            age INTEGER,
            created_at TIMESTAMP
        )
    """)
    conn.raw_sql("""
        INSERT INTO users VALUES
        (1, 'alice@example.com', 'active', 25, CURRENT_TIMESTAMP),
        (2, NULL, 'inactive', 30, CURRENT_TIMESTAMP - INTERVAL '1 hour'),
        (3, 'charlie@example.com', 'active', -5, CURRENT_TIMESTAMP),
        (4, 'dave@example.com', 'unknown', 40, CURRENT_TIMESTAMP - INTERVAL '48 hours'),
        (5, 'eve@example.com', 'active', 35, NULL)
    """)
    conn.raw_sql("""
        CREATE TABLE orders (
            order_id INTEGER,
            user_id INTEGER,
            total DECIMAL(10,2)
        )
    """)
    conn.raw_sql("""
        INSERT INTO orders VALUES
        (1, 1, 100.00),
        (2, 2, 50.00),
        (3, 999, -10.00),
        (4, 1, 200.00)
    """)
    return conn


@pytest.fixture
def duckdb_conn_with_duplicates(duckdb_conn: ibis.BaseBackend) -> ibis.BaseBackend:
    """Extend the base fixture by inserting a duplicate id into users."""
    duckdb_conn.raw_sql("INSERT INTO users VALUES (1, 'dup@example.com', 'active', 22, CURRENT_TIMESTAMP)")
    return duckdb_conn


@pytest.fixture
def empty_duckdb_conn() -> ibis.BaseBackend:
    """DuckDB connection with an empty table for SKIPPED-scenario tests."""
    conn = ibis.duckdb.connect()
    conn.raw_sql("""
        CREATE TABLE empty_table (
            id INTEGER,
            name VARCHAR,
            score INTEGER,
            created_at TIMESTAMP
        )
    """)
    return conn


# ---------------------------------------------------------------------------
# NotNullCheck
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestNotNullCheck:
    def test_passes_on_column_with_no_nulls(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = NotNullCheck(column="id")
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED
        assert result.failed_rows == 0
        assert result.total_rows == 5

    def test_fails_on_column_with_nulls(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = NotNullCheck(column="email")
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows == 1
        assert result.total_rows == 5

    def test_fails_on_timestamp_column_with_nulls(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = NotNullCheck(column="created_at")
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows == 1


# ---------------------------------------------------------------------------
# UniqueCheck
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUniqueCheck:
    def test_passes_on_unique_column(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = UniqueCheck(column="id")
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED
        assert result.failed_rows == 0

    def test_fails_with_duplicate_ids(self, duckdb_conn_with_duplicates: ibis.BaseBackend) -> None:
        check = UniqueCheck(column="id")
        result = check.run(duckdb_conn_with_duplicates, "users")
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows > 0

    def test_passes_on_unique_order_id(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = UniqueCheck(column="order_id")
        result = check.run(duckdb_conn, "orders")
        assert result.status == CheckStatus.PASSED


# ---------------------------------------------------------------------------
# AcceptedValuesCheck
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestAcceptedValuesCheck:
    def test_passes_when_all_values_accepted(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = AcceptedValuesCheck(column="status", values=["active", "inactive", "unknown"])
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED
        assert result.failed_rows == 0

    def test_fails_when_value_not_in_accepted_set(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = AcceptedValuesCheck(column="status", values=["active", "inactive"])
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.FAILED
        # Row 4 has status='unknown' which is not accepted
        assert result.failed_rows >= 1

    def test_null_not_counted_by_isin(self, duckdb_conn: ibis.BaseBackend) -> None:
        """NULLs are excluded by SQL three-valued logic in isin(); only non-null mismatches count."""
        check = AcceptedValuesCheck(column="email", values=["alice@example.com", "charlie@example.com"])
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.FAILED
        # dave@example.com and eve@example.com are not in the accepted set (NULL is not counted)
        assert result.failed_rows == 2


# ---------------------------------------------------------------------------
# RowCountCheck
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestRowCountCheck:
    def test_passes_with_min_count_satisfied(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = RowCountCheck(min_count=1)
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED

    def test_fails_when_min_count_not_met(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = RowCountCheck(min_count=100)
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.FAILED

    def test_passes_with_max_count_satisfied(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = RowCountCheck(max_count=10)
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED

    def test_fails_when_max_count_exceeded(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = RowCountCheck(max_count=2)
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.FAILED

    def test_passes_with_both_bounds_satisfied(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = RowCountCheck(min_count=1, max_count=10)
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED


# ---------------------------------------------------------------------------
# FreshnessCheck
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestFreshnessCheck:
    def test_passes_when_data_is_fresh(self, duckdb_conn: ibis.BaseBackend) -> None:
        # Most recent row is CURRENT_TIMESTAMP, so 72h is plenty.
        check = FreshnessCheck(column="created_at", max_age_hours=72)
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED

    def test_passes_with_generous_max_age(self, duckdb_conn: ibis.BaseBackend) -> None:
        # The most recent non-null created_at is ~CURRENT_TIMESTAMP at INSERT time.
        # A 24-hour window comfortably covers it.
        check = FreshnessCheck(column="created_at", max_age_hours=24)
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED

    def test_fails_on_explicitly_stale_data(self, duckdb_conn: ibis.BaseBackend) -> None:
        """Create a table where ALL timestamps are old, guaranteeing staleness."""
        duckdb_conn.raw_sql("""
            CREATE TABLE stale_events (
                id INTEGER,
                event_ts TIMESTAMP
            )
        """)
        duckdb_conn.raw_sql("""
            INSERT INTO stale_events VALUES
            (1, CURRENT_TIMESTAMP - INTERVAL '7 days'),
            (2, CURRENT_TIMESTAMP - INTERVAL '10 days')
        """)
        check = FreshnessCheck(column="event_ts", max_age_hours=24)
        result = check.run(duckdb_conn, "stale_events")
        assert result.status == CheckStatus.FAILED

    def test_skipped_on_empty_table(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        check = FreshnessCheck(column="created_at", max_age_hours=24)
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.SKIPPED


# ---------------------------------------------------------------------------
# ExpressionCheck
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestExpressionCheck:
    def test_passes_when_all_rows_satisfy_expression(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = ExpressionCheck(
            expression=lambda t: t["id"] > 0,
            name="positive_id",
        )
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED
        assert result.failed_rows == 0

    def test_fails_when_some_rows_violate_expression(self, duckdb_conn: ibis.BaseBackend) -> None:
        # User 3 has age=-5, which violates age > 0
        check = ExpressionCheck(
            expression=lambda t: t["age"] > 0,
            name="positive_age",
        )
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows >= 1

    def test_skipped_on_empty_table(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        check = ExpressionCheck(
            expression=lambda t: t["id"] > 0,
            name="positive_id",
        )
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.SKIPPED


# ---------------------------------------------------------------------------
# RelationshipsCheck
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestRelationshipsCheck:
    def test_passes_when_all_foreign_keys_exist(self, duckdb_conn: ibis.BaseBackend) -> None:
        """Only user_ids 1 and 2 are referenced; both exist in users."""
        # Remove the orphan row so everything matches.
        duckdb_conn.raw_sql("DELETE FROM orders WHERE user_id = 999")
        check = RelationshipsCheck(column="user_id", to_table="users", to_column="id")
        result = check.run(duckdb_conn, "orders")
        assert result.status == CheckStatus.PASSED
        assert result.failed_rows == 0

    def test_fails_when_orphaned_foreign_key_exists(self, duckdb_conn: ibis.BaseBackend) -> None:
        # user_id=999 in orders does not exist in users
        check = RelationshipsCheck(column="user_id", to_table="users", to_column="id")
        result = check.run(duckdb_conn, "orders")
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows >= 1

    def test_skipped_on_empty_child_table(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        empty_duckdb_conn.raw_sql("CREATE TABLE parent (id INTEGER)")
        empty_duckdb_conn.raw_sql("INSERT INTO parent VALUES (1)")
        check = RelationshipsCheck(column="id", to_table="parent", to_column="id")
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.SKIPPED


# ---------------------------------------------------------------------------
# PatternCheck
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestPatternCheck:
    def test_passes_when_all_rows_match(self, duckdb_conn: ibis.BaseBackend) -> None:
        # Every status value is a non-empty string, so ".+" matches all.
        check = PatternCheck(column="status", pattern=".+")
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED
        assert result.failed_rows == 0

    def test_fails_when_null_present(self, duckdb_conn: ibis.BaseBackend) -> None:
        # email column has 1 NULL -- NULLs are treated as pattern failures.
        check = PatternCheck(column="email", pattern=r"^[^@]+@[^@]+\.[^@]+$")
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows >= 1

    def test_fails_on_non_matching_values(self, duckdb_conn: ibis.BaseBackend) -> None:
        # Pattern requires digits only; status values are alphabetic.
        check = PatternCheck(column="status", pattern=r"^\d+$")
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows == 5

    def test_skipped_on_empty_table(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        check = PatternCheck(column="name", pattern=".+")
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.SKIPPED


# ---------------------------------------------------------------------------
# RangeCheck
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestRangeCheck:
    def test_passes_when_all_in_range(self, duckdb_conn: ibis.BaseBackend) -> None:
        # All ids are in [1, 5], all ages are in [-10, 100].
        check = RangeCheck(column="id", min_value=0, max_value=100)
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED
        assert result.failed_rows == 0

    def test_fails_when_value_below_min(self, duckdb_conn: ibis.BaseBackend) -> None:
        # age has -5, which is below min_value=0
        check = RangeCheck(column="age", min_value=0, max_value=100)
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows >= 1

    def test_fails_when_value_above_max(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = RangeCheck(column="age", min_value=-100, max_value=30)
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.FAILED
        # ages 35 and 40 exceed max=30
        assert result.failed_rows >= 2

    def test_one_sided_min_only(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = RangeCheck(column="id", min_value=1)
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED

    def test_skipped_on_empty_table(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        check = RangeCheck(column="score", min_value=0, max_value=100)
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.SKIPPED


# ---------------------------------------------------------------------------
# SqlCheck
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSqlCheck:
    def test_passes_when_no_failing_rows(self, duckdb_conn: ibis.BaseBackend) -> None:
        # No user has id < 0, so the query returns 0 rows.
        check = SqlCheck(sql="SELECT * FROM users WHERE id < 0", name="no_negative_ids")
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED
        assert result.failed_rows == 0

    def test_fails_when_failing_rows_exist(self, duckdb_conn: ibis.BaseBackend) -> None:
        # Order 3 has total=-10.00
        check = SqlCheck(sql="SELECT * FROM orders WHERE total < 0", name="no_negative_totals")
        result = check.run(duckdb_conn, "orders")
        assert result.status == CheckStatus.FAILED
        assert result.failed_rows >= 1

    def test_table_placeholder_substitution(self, duckdb_conn: ibis.BaseBackend) -> None:
        check = SqlCheck(sql="SELECT * FROM {table} WHERE id < 0", name="no_negative_ids")
        result = check.run(duckdb_conn, "users")
        assert result.status == CheckStatus.PASSED


# ---------------------------------------------------------------------------
# CheckRunner end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCheckRunner:
    def test_run_checks_mixed_results(self, duckdb_conn: ibis.BaseBackend) -> None:
        """Run several checks and verify the summary tallies."""
        runner = CheckRunner(connection=duckdb_conn)
        checks = [
            NotNullCheck(column="id"),  # PASS
            NotNullCheck(column="email"),  # FAIL (1 null)
            UniqueCheck(column="id"),  # PASS
            AcceptedValuesCheck(column="status", values=["active", "inactive"]),  # FAIL ('unknown')
            RowCountCheck(min_count=1),  # PASS
        ]
        summary: CheckSummary = runner.run_checks(table_name="users", checks=checks)

        assert summary.total_checks == 5
        assert summary.passed == 3
        assert summary.failed == 2
        assert summary.errors == 0
        assert summary.has_failures is True
        assert len(summary.results) == 5
        assert summary.duration_seconds > 0

    def test_run_checks_all_pass(self, duckdb_conn: ibis.BaseBackend) -> None:
        runner = CheckRunner(connection=duckdb_conn)
        checks = [
            NotNullCheck(column="id"),
            UniqueCheck(column="id"),
            RowCountCheck(min_count=1, max_count=100),
        ]
        summary = runner.run_checks(table_name="users", checks=checks)

        assert summary.passed == 3
        assert summary.failed == 0
        assert summary.has_failures is False

    def test_fail_fast_stops_after_first_error_severity_failure(self, duckdb_conn: ibis.BaseBackend) -> None:
        runner = CheckRunner(connection=duckdb_conn, fail_fast=True)
        checks: list[Check] = [
            NotNullCheck(column="email"),  # FAIL (severity=ERROR)
            NotNullCheck(column="id"),  # Would pass, but runner should stop
        ]
        summary = runner.run_checks(table_name="users", checks=checks)

        # fail_fast breaks after first ERROR-severity failure
        assert summary.failed >= 1
        assert len(summary.results) == 1

    def test_summary_to_dict(self, duckdb_conn: ibis.BaseBackend) -> None:
        runner = CheckRunner(connection=duckdb_conn)
        summary = runner.run_checks(
            table_name="users",
            checks=[NotNullCheck(column="id")],
        )
        d = summary.to_dict()
        assert d["table_name"] == "users"
        assert d["total_checks"] == 1
        assert d["passed"] == 1
        assert "results" in d
        assert isinstance(d["results"], list)

    def test_connection_override(self, duckdb_conn: ibis.BaseBackend) -> None:
        """Runner created without a connection can accept one at call time."""
        runner = CheckRunner()
        summary = runner.run_checks(
            table_name="users",
            checks=[RowCountCheck(min_count=1)],
            connection=duckdb_conn,
        )
        assert summary.passed == 1


# ---------------------------------------------------------------------------
# CheckRunner._parse_check_configs
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCheckRunnerParseConfigs:
    def test_parse_basic_check_types(self, duckdb_conn: ibis.BaseBackend) -> None:
        """Config dicts are parsed into the correct Check subclasses and run."""
        runner = CheckRunner(connection=duckdb_conn)
        configs: list[dict[str, Any]] = [
            {"type": "not_null", "column": "id"},
            {"type": "unique", "column": "id"},
            {"type": "accepted_values", "column": "status", "values": ["active", "inactive", "unknown"]},
            {"type": "row_count", "min_count": 1},
        ]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 4
        assert isinstance(checks[0], NotNullCheck)
        assert isinstance(checks[1], UniqueCheck)
        assert isinstance(checks[2], AcceptedValuesCheck)
        assert isinstance(checks[3], RowCountCheck)

    def test_parse_advanced_check_types(self, duckdb_conn: ibis.BaseBackend) -> None:
        runner = CheckRunner(connection=duckdb_conn)
        configs: list[dict[str, Any]] = [
            {"type": "freshness", "column": "created_at", "max_age_hours": 72},
            {"type": "pattern", "column": "status", "pattern": ".+"},
            {"type": "range", "column": "age", "min_value": 0, "max_value": 100},
            {"type": "relationships", "column": "user_id", "to_table": "users", "to_column": "id"},
            {"type": "sql", "sql": "SELECT * FROM users WHERE id < 0", "name": "no_negatives"},
        ]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 5
        assert isinstance(checks[0], FreshnessCheck)
        assert isinstance(checks[1], PatternCheck)
        assert isinstance(checks[2], RangeCheck)
        assert isinstance(checks[3], RelationshipsCheck)
        assert isinstance(checks[4], SqlCheck)

    def test_parse_with_severity(self, duckdb_conn: ibis.BaseBackend) -> None:
        runner = CheckRunner(connection=duckdb_conn)
        from interlace.checks.base import CheckSeverity

        configs = [
            {"type": "not_null", "column": "email", "severity": "warn"},
            {"type": "not_null", "column": "id", "severity": "error"},
            {"type": "not_null", "column": "status", "severity": "info"},
        ]
        checks = runner._parse_check_configs(configs)
        assert checks[0].severity == CheckSeverity.WARN
        assert checks[1].severity == CheckSeverity.ERROR
        assert checks[2].severity == CheckSeverity.INFO

    def test_parse_unknown_type_is_skipped(self, duckdb_conn: ibis.BaseBackend) -> None:
        runner = CheckRunner(connection=duckdb_conn)
        configs = [
            {"type": "nonexistent_check", "column": "id"},
            {"type": "not_null", "column": "id"},
        ]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 1
        assert isinstance(checks[0], NotNullCheck)

    def test_parse_missing_type_key_is_skipped(self, duckdb_conn: ibis.BaseBackend) -> None:
        runner = CheckRunner(connection=duckdb_conn)
        configs = [
            {"column": "id"},  # missing 'type'
        ]
        checks = runner._parse_check_configs(configs)
        assert len(checks) == 0

    def test_run_model_checks_with_config_dicts(self, duckdb_conn: ibis.BaseBackend) -> None:
        """End-to-end: run_model_checks with config dict input."""
        runner = CheckRunner(connection=duckdb_conn)
        model_info = {
            "checks": [
                {"type": "not_null", "column": "id"},
                {"type": "unique", "column": "id"},
                {"type": "row_count", "min_count": 1},
            ],
        }
        summary = runner.run_model_checks(model_name="users", model_info=model_info)
        assert summary is not None
        assert summary.passed == 3
        assert summary.failed == 0

    def test_run_model_checks_with_check_instances(self, duckdb_conn: ibis.BaseBackend) -> None:
        """run_model_checks also accepts pre-built Check instances."""
        runner = CheckRunner(connection=duckdb_conn)
        model_info = {
            "checks": [
                NotNullCheck(column="id"),
                UniqueCheck(column="id"),
            ],
        }
        summary = runner.run_model_checks(model_name="users", model_info=model_info)
        assert summary is not None
        assert summary.passed == 2

    def test_run_model_checks_returns_none_when_no_checks(self, duckdb_conn: ibis.BaseBackend) -> None:
        runner = CheckRunner(connection=duckdb_conn)
        summary = runner.run_model_checks(model_name="users", model_info={})
        assert summary is None


# ---------------------------------------------------------------------------
# Empty table scenarios
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestEmptyTableScenarios:
    """Most checks should return SKIPPED when the table has no rows."""

    def test_expression_skipped(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        check = ExpressionCheck(expression=lambda t: t["id"] > 0, name="pos_id")
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.SKIPPED

    def test_freshness_skipped(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        check = FreshnessCheck(column="created_at", max_age_hours=24)
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.SKIPPED

    def test_pattern_skipped(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        check = PatternCheck(column="name", pattern=".+")
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.SKIPPED

    def test_range_skipped(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        check = RangeCheck(column="score", min_value=0, max_value=100)
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.SKIPPED

    def test_not_null_passes_on_empty(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        """NotNullCheck counts nulls; 0 rows means 0 nulls => PASSED."""
        check = NotNullCheck(column="id")
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.PASSED
        assert result.total_rows == 0

    def test_unique_passes_on_empty(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        """UniqueCheck counts duplicates; 0 rows means 0 duplicates => PASSED."""
        check = UniqueCheck(column="id")
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.PASSED

    def test_row_count_fails_min_on_empty(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        """RowCountCheck with min_count=1 fails on an empty table."""
        check = RowCountCheck(min_count=1)
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.FAILED

    def test_row_count_passes_max_on_empty(self, empty_duckdb_conn: ibis.BaseBackend) -> None:
        """RowCountCheck with max_count=10 passes on an empty table."""
        check = RowCountCheck(max_count=10)
        result = check.run(empty_duckdb_conn, "empty_table")
        assert result.status == CheckStatus.PASSED
