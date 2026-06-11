"""
Check runner.

Orchestrates execution of data checks on tables.
"""

import time
from dataclasses import dataclass, field
from typing import Any

import ibis

from interlace.checks.base import (
    Check,
    CheckResult,
    CheckSeverity,
    CheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.checks.runner")


@dataclass
class CheckSummary:
    """
    Summary of check execution.

    Attributes:
        table_name: Table that was checked
        total_checks: Number of checks run
        passed: Number of checks that passed
        failed: Number of checks that failed
        errors: Number of checks that errored
        skipped: Number of checks that were skipped
        results: Individual check results
        duration_seconds: Total execution time
    """

    table_name: str
    total_checks: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0
    skipped: int = 0
    results: list[CheckResult] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def has_failures(self) -> bool:
        """Check if any ERROR severity checks failed."""
        return any(r.status == CheckStatus.FAILED and r.severity == CheckSeverity.ERROR for r in self.results)

    @property
    def has_warnings(self) -> bool:
        """Check if any WARN severity checks failed."""
        return any(r.status == CheckStatus.FAILED and r.severity == CheckSeverity.WARN for r in self.results)

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_checks == 0:
            return 100.0
        return (self.passed / self.total_checks) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert summary to dictionary."""
        return {
            "table_name": self.table_name,
            "total_checks": self.total_checks,
            "passed": self.passed,
            "failed": self.failed,
            "errors": self.errors,
            "skipped": self.skipped,
            "success_rate": self.success_rate,
            "has_failures": self.has_failures,
            "has_warnings": self.has_warnings,
            "duration_seconds": self.duration_seconds,
            "results": [r.to_dict() for r in self.results],
        }


class CheckRunner:
    """
    Runs data checks on tables.

    Orchestrates check execution, collects results, and provides summaries.

    Usage:
        runner = CheckRunner(connection)
        results = runner.run_checks(
            table_name="users",
            checks=[UniqueCheck(column="user_id"), NotNullCheck(column="email")],
        )
    """

    def __init__(
        self,
        connection: ibis.BaseBackend | None = None,
        fail_fast: bool = False,
    ):
        self.connection = connection
        self.fail_fast = fail_fast

    def run_checks(
        self,
        table_name: str,
        checks: list[Check],
        schema: str | None = None,
        connection: ibis.BaseBackend | None = None,
    ) -> CheckSummary:
        """
        Run checks on a table.

        Args:
            table_name: Name of the table to check
            checks: List of checks to run
            schema: Schema containing the table
            connection: Optional connection override

        Returns:
            CheckSummary with all results
        """
        conn = connection or self.connection
        if conn is None:
            raise ValueError("No connection provided")

        start_time = time.time()
        summary = CheckSummary(table_name=table_name, total_checks=len(checks))

        for check in checks:
            try:
                result = check.run(conn, table_name, schema)
                summary.results.append(result)

                if result.status == CheckStatus.PASSED:
                    summary.passed += 1
                elif result.status == CheckStatus.FAILED:
                    summary.failed += 1
                    msg = f"{table_name} > {check.check_name}: {result.message}"
                    if result.severity == CheckSeverity.ERROR:
                        logger.error(msg)
                    elif result.severity == CheckSeverity.WARN:
                        logger.warning(msg)
                    else:
                        logger.info(msg)

                    if self.fail_fast and result.severity == CheckSeverity.ERROR:
                        break
                elif result.status == CheckStatus.ERROR:
                    summary.errors += 1
                    logger.error(f"{table_name} > {check.check_name}: {result.message}")
                elif result.status == CheckStatus.SKIPPED:
                    summary.skipped += 1
                    logger.debug(f"{table_name} > {check.check_name}: skipped")

            except Exception as e:
                logger.error(f"{table_name} > {check.check_name}: unexpected error: {e}")
                summary.errors += 1
                summary.results.append(
                    CheckResult(
                        check_name=check.check_name,
                        check_type=check.check_type,
                        status=CheckStatus.ERROR,
                        severity=check.severity,
                        table_name=table_name,
                        message=f"Unexpected error: {str(e)}",
                    )
                )

        summary.duration_seconds = time.time() - start_time

        if summary.has_failures:
            logger.error(
                f"Checks for '{table_name}': "
                f"{summary.passed}/{summary.total_checks} passed, "
                f"{summary.failed} failed ({summary.duration_seconds:.2f}s)"
            )
        else:
            logger.info(
                f"Checks for '{table_name}': "
                f"{summary.passed}/{summary.total_checks} passed "
                f"({summary.duration_seconds:.2f}s)"
            )

        return summary

    def run_model_checks(
        self,
        model_name: str,
        model_info: dict[str, Any],
        schema: str | None = None,
        connection: ibis.BaseBackend | None = None,
    ) -> CheckSummary | None:
        """
        Run checks defined in model configuration.

        Args:
            model_name: Name of the model
            model_info: Model configuration dictionary
            schema: Schema containing the model's table
            connection: Optional connection override

        Returns:
            CheckSummary or None if no checks defined
        """
        checks_config = model_info.get("checks", [])
        if not checks_config:
            return None

        # Separate Check instances from config dicts
        check_instances: list[Check] = []
        config_dicts: list[dict[str, Any]] = []
        for item in checks_config:
            if isinstance(item, Check):
                check_instances.append(item)
            elif isinstance(item, dict) and "_check_instance" in item:
                check_instances.append(item["_check_instance"])
            else:
                config_dicts.append(item)

        parsed = self._parse_check_configs(config_dicts)
        all_checks = check_instances + parsed

        if not all_checks:
            return None

        return self.run_checks(
            table_name=model_name,
            checks=all_checks,
            schema=schema,
            connection=connection,
        )

    def _parse_check_configs(self, configs: list[dict[str, Any]]) -> list[Check]:
        """Parse check configuration dictionaries into Check objects."""
        from interlace.checks.types import (
            AcceptedValuesCheck,
            ExpressionCheck,
            FreshnessCheck,
            NotNullCheck,
            PatternCheck,
            RangeCheck,
            RelationshipsCheck,
            RowCountCheck,
            SqlCheck,
            UniqueCheck,
        )

        check_types: dict[str, type[Check]] = {
            "unique": UniqueCheck,
            "not_null": NotNullCheck,
            "accepted_values": AcceptedValuesCheck,
            "freshness": FreshnessCheck,
            "row_count": RowCountCheck,
            "expression": ExpressionCheck,
            "relationships": RelationshipsCheck,
            "pattern": PatternCheck,
            "range": RangeCheck,
            "sql": SqlCheck,
        }

        checks: list[Check] = []
        for config in configs:
            check_type = config.get("type")
            if not check_type:
                logger.warning(f"Check config missing 'type': {config}")
                continue

            check_class = check_types.get(check_type)
            if not check_class:
                logger.warning(f"Unknown check type: {check_type}")
                continue

            # Parse severity
            severity_str = config.get("severity", "error").lower()
            severity_map = {
                "error": CheckSeverity.ERROR,
                "warn": CheckSeverity.WARN,
                "warning": CheckSeverity.WARN,
                "info": CheckSeverity.INFO,
            }
            severity = severity_map.get(severity_str, CheckSeverity.ERROR)

            # Build check kwargs
            kwargs = {k: v for k, v in config.items() if k not in ("type", "severity")}
            kwargs["severity"] = severity

            try:
                check = check_class(**kwargs)
                checks.append(check)
            except Exception as e:
                logger.warning(f"Error creating {check_type} check: {e}")

        return checks
