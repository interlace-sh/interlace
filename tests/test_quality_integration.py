"""Tests for quality check integration into the execution pipeline."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from interlace.quality.base import QualityCheckResult, QualityCheckSeverity, QualityCheckStatus
from interlace.quality.runner import QualityCheckRunner, QualityCheckSummary


@pytest.mark.unit
class TestModelDecoratorQualityParam:
    """Test that @model accepts quality_checks parameter."""

    def test_quality_checks_stored_in_metadata(self) -> None:
        from interlace.core.model import model

        @model(name="test_model", quality_checks=[{"type": "not_null", "column": "id"}])
        def my_model() -> None:
            pass

        meta = my_model._interlace_model  # type: ignore[attr-defined]
        assert meta["quality_checks"] == [{"type": "not_null", "column": "id"}]

    def test_no_quality_checks_default_none(self) -> None:
        from interlace.core.model import model

        @model(name="test_model2")
        def my_model() -> None:
            pass

        meta = my_model._interlace_model  # type: ignore[attr-defined]
        assert meta["quality_checks"] is None


@pytest.mark.unit
class TestQualityCheckRunnerModelChecks:
    """Test QualityCheckRunner.run_model_checks with config parsing."""

    def test_no_checks_returns_none(self) -> None:
        runner = QualityCheckRunner()
        result = runner.run_model_checks("test", {}, connection=MagicMock())
        assert result is None

    def test_empty_checks_returns_none(self) -> None:
        runner = QualityCheckRunner()
        result = runner.run_model_checks("test", {"quality_checks": []}, connection=MagicMock())
        assert result is None

    def test_parses_not_null_check(self) -> None:
        runner = QualityCheckRunner()
        checks = runner._parse_check_configs([{"type": "not_null", "column": "id", "severity": "error"}])
        assert len(checks) == 1
        assert checks[0].check_type == "not_null"
        assert checks[0].column == "id"
        assert checks[0].severity == QualityCheckSeverity.ERROR

    def test_parses_unique_check(self) -> None:
        runner = QualityCheckRunner()
        checks = runner._parse_check_configs([{"type": "unique", "column": "email"}])
        assert len(checks) == 1
        assert checks[0].check_type == "unique"

    def test_parses_warn_severity(self) -> None:
        runner = QualityCheckRunner()
        checks = runner._parse_check_configs([{"type": "not_null", "column": "name", "severity": "warn"}])
        assert checks[0].severity == QualityCheckSeverity.WARN

    def test_unknown_type_skipped(self) -> None:
        runner = QualityCheckRunner()
        checks = runner._parse_check_configs([{"type": "bogus"}])
        assert len(checks) == 0

    def test_missing_type_skipped(self) -> None:
        runner = QualityCheckRunner()
        checks = runner._parse_check_configs([{"column": "id"}])
        assert len(checks) == 0


@pytest.mark.unit
class TestQualitySummaryProperties:
    """Test QualityCheckSummary computed properties."""

    def _make_result(
        self,
        status: QualityCheckStatus = QualityCheckStatus.PASSED,
        severity: QualityCheckSeverity = QualityCheckSeverity.ERROR,
    ) -> QualityCheckResult:
        return QualityCheckResult(
            check_name="test",
            check_type="not_null",
            status=status,
            severity=severity,
            table_name="my_table",
        )

    def test_has_failures_true(self) -> None:
        summary = QualityCheckSummary(
            table_name="test",
            total_checks=2,
            passed=1,
            failed=1,
            results=[
                self._make_result(QualityCheckStatus.PASSED),
                self._make_result(QualityCheckStatus.FAILED, QualityCheckSeverity.ERROR),
            ],
        )
        assert summary.has_failures is True

    def test_has_failures_false_when_only_warn(self) -> None:
        summary = QualityCheckSummary(
            table_name="test",
            total_checks=1,
            failed=1,
            results=[self._make_result(QualityCheckStatus.FAILED, QualityCheckSeverity.WARN)],
        )
        assert summary.has_failures is False
        assert summary.has_warnings is True

    def test_success_rate(self) -> None:
        summary = QualityCheckSummary(table_name="test", total_checks=4, passed=3, failed=1)
        assert summary.success_rate == 75.0

    def test_to_dict(self) -> None:
        summary = QualityCheckSummary(table_name="test", total_checks=1, passed=1)
        d = summary.to_dict()
        assert d["table_name"] == "test"
        assert d["total_checks"] == 1
        assert d["passed"] == 1


@pytest.mark.unit
class TestConfigMerge:
    """Test that quality checks from config.yaml are merged into model_info."""

    def test_config_checks_merged(self) -> None:
        """Simulate what initialization.py does."""
        models: dict[str, dict[str, Any]] = {
            "users": {"name": "users", "quality_checks": None},
            "orders": {"name": "orders", "quality_checks": None},
        }

        quality_config = {
            "enabled": True,
            "fail_on_error": True,
            "checks": {
                "users": [{"type": "not_null", "column": "id"}],
            },
        }

        # Simulate the merge logic from initialization.py
        if quality_config.get("enabled", True):
            fail_on_error = quality_config.get("fail_on_error", False)
            checks_by_model = quality_config.get("checks", {})
            if isinstance(checks_by_model, dict):
                for model_name, model_info in models.items():
                    if model_name in checks_by_model and not model_info.get("quality_checks"):
                        model_info["quality_checks"] = checks_by_model[model_name]
                    if model_info.get("quality_checks"):
                        model_info.setdefault("quality_fail_on_error", fail_on_error)

        assert models["users"]["quality_checks"] == [{"type": "not_null", "column": "id"}]
        assert models["users"]["quality_fail_on_error"] is True
        assert models["orders"].get("quality_checks") is None

    def test_decorator_takes_precedence(self) -> None:
        """Decorator-level checks should NOT be overwritten by config."""
        models: dict[str, dict[str, Any]] = {
            "users": {"name": "users", "quality_checks": [{"type": "unique", "column": "email"}]},
        }

        quality_config = {
            "enabled": True,
            "checks": {"users": [{"type": "not_null", "column": "id"}]},
        }

        # Simulate merge
        checks_by_model = quality_config.get("checks", {})
        if isinstance(checks_by_model, dict):
            for model_name, model_info in models.items():
                if model_name in checks_by_model and not model_info.get("quality_checks"):
                    model_info["quality_checks"] = checks_by_model[model_name]

        # Decorator's checks should be preserved
        assert models["users"]["quality_checks"] == [{"type": "unique", "column": "email"}]


@pytest.mark.unit
class TestQualityResultsStorage:
    """Test quality results table creation and querying."""

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

    def test_quality_results_table_exists(self, store: Any) -> None:
        conn = store._get_connection()
        assert conn is not None
        # Should not raise
        result = conn.sql("SELECT COUNT(*) as cnt FROM interlace.quality_results").execute()
        assert result.iloc[0]["cnt"] == 0

    def test_insert_and_query_quality_results(self, store: Any) -> None:
        from interlace.core.context import _execute_sql_internal
        from interlace.core.state import _sql_value

        conn = store._get_connection()
        _execute_sql_internal(
            conn,
            f"INSERT INTO interlace.quality_results "
            f"(check_name, check_type, model_name, schema_name, status, severity, "
            f"message, failed_rows, total_rows, duration_seconds, flow_id, task_id) "
            f"VALUES ({_sql_value('not_null_id')}, {_sql_value('not_null')}, "
            f"{_sql_value('users')}, {_sql_value('public')}, {_sql_value('passed')}, "
            f"{_sql_value('error')}, {_sql_value('All rows passed')}, 0, 100, 0.05, "
            f"{_sql_value('flow-1')}, {_sql_value('task-1')})",
        )

        result = conn.sql("SELECT * FROM interlace.quality_results WHERE model_name = 'users'").execute()
        assert len(result) == 1
        row = result.iloc[0]
        assert row["check_name"] == "not_null_id"
        assert row["status"] == "passed"
        assert row["total_rows"] == 100
