"""Tests for checks integration into the execution pipeline."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from interlace.checks.base import CheckResult, CheckSeverity, CheckStatus
from interlace.checks.runner import CheckRunner, CheckSummary


@pytest.mark.unit
class TestModelDecoratorChecksParam:
    """Test that @model accepts checks parameter."""

    def test_checks_stored_in_metadata(self) -> None:
        from interlace.core.model import model

        @model(name="test_model_checks", checks=[{"type": "not_null", "column": "id"}])
        def my_model() -> None:
            pass

        meta = my_model._interlace_model  # type: ignore[attr-defined]
        assert meta["checks"] == [{"type": "not_null", "column": "id"}]

    def test_no_checks_default_none(self) -> None:
        from interlace.core.model import model

        @model(name="test_model_checks2")
        def my_model() -> None:
            pass

        meta = my_model._interlace_model  # type: ignore[attr-defined]
        assert meta["checks"] is None


@pytest.mark.unit
class TestCheckRunnerModelChecks:
    """Test CheckRunner.run_model_checks with config parsing."""

    def test_no_checks_returns_none(self) -> None:
        runner = CheckRunner()
        result = runner.run_model_checks("test", {}, connection=MagicMock())
        assert result is None

    def test_empty_checks_returns_none(self) -> None:
        runner = CheckRunner()
        result = runner.run_model_checks("test", {"checks": []}, connection=MagicMock())
        assert result is None

    def test_parses_not_null_check(self) -> None:
        runner = CheckRunner()
        checks = runner._parse_check_configs([{"type": "not_null", "column": "id", "severity": "error"}])
        assert len(checks) == 1
        assert checks[0].check_type == "not_null"
        assert checks[0].column == "id"
        assert checks[0].severity == CheckSeverity.ERROR

    def test_parses_unique_check(self) -> None:
        runner = CheckRunner()
        checks = runner._parse_check_configs([{"type": "unique", "column": "email"}])
        assert len(checks) == 1
        assert checks[0].check_type == "unique"

    def test_parses_warn_severity(self) -> None:
        runner = CheckRunner()
        checks = runner._parse_check_configs([{"type": "not_null", "column": "name", "severity": "warn"}])
        assert checks[0].severity == CheckSeverity.WARN


@pytest.mark.unit
class TestCheckSummaryProperties:
    """Test CheckSummary computed properties."""

    def _make_result(
        self,
        status: CheckStatus = CheckStatus.PASSED,
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> CheckResult:
        return CheckResult(
            check_name="test",
            check_type="not_null",
            status=status,
            severity=severity,
            table_name="my_table",
        )

    def test_has_failures_true(self) -> None:
        summary = CheckSummary(
            table_name="test",
            total_checks=2,
            passed=1,
            failed=1,
            results=[
                self._make_result(CheckStatus.PASSED),
                self._make_result(CheckStatus.FAILED, CheckSeverity.ERROR),
            ],
        )
        assert summary.has_failures is True

    def test_has_failures_false_when_only_warn(self) -> None:
        summary = CheckSummary(
            table_name="test",
            total_checks=1,
            failed=1,
            results=[self._make_result(CheckStatus.FAILED, CheckSeverity.WARN)],
        )
        assert summary.has_failures is False
        assert summary.has_warnings is True

    def test_success_rate(self) -> None:
        summary = CheckSummary(table_name="test", total_checks=4, passed=3, failed=1)
        assert summary.success_rate == 75.0

    def test_to_dict(self) -> None:
        summary = CheckSummary(table_name="test", total_checks=1, passed=1)
        d = summary.to_dict()
        assert d["table_name"] == "test"
        assert d["total_checks"] == 1
        assert d["passed"] == 1


@pytest.mark.unit
class TestConfigMerge:
    """Test that checks from config.yaml are merged into model_info."""

    def test_config_checks_merged(self) -> None:
        """Simulate what initialization.py does with the new config format."""
        models: dict[str, dict[str, Any]] = {
            "users": {"name": "users", "checks": None},
            "orders": {"name": "orders", "checks": None},
        }

        checks_config = {
            "enabled": True,
            "fail_on_error": True,
            "models": {
                "users": [{"type": "not_null", "column": "id"}],
            },
        }

        # Simulate the merge logic from initialization.py
        if checks_config.get("enabled", True):
            fail_on_error = checks_config.get("fail_on_error", False)
            checks_by_model = checks_config.get("models", {})
            if isinstance(checks_by_model, dict):
                for model_name, model_info in models.items():
                    if model_name in checks_by_model and not model_info.get("checks"):
                        model_info["checks"] = checks_by_model[model_name]
                    if model_info.get("checks"):
                        model_info.setdefault("checks_fail_on_error", fail_on_error)

        assert models["users"]["checks"] == [{"type": "not_null", "column": "id"}]
        assert models["users"]["checks_fail_on_error"] is True
        assert models["orders"].get("checks") is None

    def test_decorator_takes_precedence(self) -> None:
        """Decorator-level checks should NOT be overwritten by config."""
        models: dict[str, dict[str, Any]] = {
            "users": {"name": "users", "checks": [{"type": "unique", "column": "email"}]},
        }

        checks_config = {
            "enabled": True,
            "models": {"users": [{"type": "not_null", "column": "id"}]},
        }

        # Simulate merge
        checks_by_model = checks_config.get("models", {})
        if isinstance(checks_by_model, dict):
            for model_name, model_info in models.items():
                if model_name in checks_by_model and not model_info.get("checks"):
                    model_info["checks"] = checks_by_model[model_name]

        # Decorator's checks should be preserved
        assert models["users"]["checks"] == [{"type": "unique", "column": "email"}]


@pytest.mark.unit
class TestCheckResultsStorage:
    """Test check results table creation and querying."""

    @pytest.fixture
    def store(self) -> Any:
        """Create a StateStore with DuckDB for testing."""
        import ibis

        from interlace.core.state import StateStore

        con = ibis.duckdb.connect()
        store = StateStore({"state": {"connection": "test"}})
        store._connection = con
        store._initialized = False
        store._initialize_schema()
        return store

    def test_check_results_table_exists(self, store: Any) -> None:
        conn = store._get_connection()
        assert conn is not None
        result = conn.sql("SELECT COUNT(*) as cnt FROM interlace.check_results").execute()
        assert result.iloc[0]["cnt"] == 0

    def test_insert_and_query_check_results(self, store: Any) -> None:
        from interlace.core.context import _execute_sql_internal
        from interlace.core.state import _sql_value

        conn = store._get_connection()
        _execute_sql_internal(
            conn,
            f"INSERT INTO interlace.check_results "
            f"(check_name, check_type, model_name, schema_name, status, severity, "
            f"message, failed_rows, total_rows, duration_seconds, flow_id, task_id) "
            f"VALUES ({_sql_value('not_null_id')}, {_sql_value('not_null')}, "
            f"{_sql_value('users')}, {_sql_value('public')}, {_sql_value('passed')}, "
            f"{_sql_value('error')}, {_sql_value('All rows passed')}, 0, 100, 0.05, "
            f"{_sql_value('flow-1')}, {_sql_value('task-1')})",
        )

        result = conn.sql("SELECT * FROM interlace.check_results WHERE model_name = 'users'").execute()
        assert len(result) == 1
        row = result.iloc[0]
        assert row["check_name"] == "not_null_id"
        assert row["status"] == "passed"
        assert row["total_rows"] == 100
