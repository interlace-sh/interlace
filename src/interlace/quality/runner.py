"""
Quality check runner.

Phase 3: Orchestrates execution of quality checks on tables.
"""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import ibis

from interlace.quality.base import (
    QualityCheck,
    QualityCheckResult,
    QualityCheckSeverity,
    QualityCheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.quality.runner")


@dataclass
class QualityCheckSummary:
    """
    Summary of quality check execution.

    Attributes:
        table_name: Table that was checked
        total_checks: Number of checks run
        passed: Number of checks that passed
        failed: Number of checks that failed
        errors: Number of checks that errored
        skipped: Number of checks that were skipped
        results: Individual check results
        duration_seconds: Total execution time
        has_failures: True if any checks failed with ERROR severity
    """

    table_name: str
    total_checks: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0
    skipped: int = 0
    results: List[QualityCheckResult] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def has_failures(self) -> bool:
        """Check if any ERROR severity checks failed."""
        return any(
            r.status == QualityCheckStatus.FAILED
            and r.severity == QualityCheckSeverity.ERROR
            for r in self.results
        )

    @property
    def has_warnings(self) -> bool:
        """Check if any WARN severity checks failed."""
        return any(
            r.status == QualityCheckStatus.FAILED
            and r.severity == QualityCheckSeverity.WARN
            for r in self.results
        )

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_checks == 0:
            return 100.0
        return (self.passed / self.total_checks) * 100

    def to_dict(self) -> Dict[str, Any]:
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


class QualityCheckRunner:
    """
    Runs quality checks on tables.

    Orchestrates check execution, collects results, and provides summaries.

    Usage:
        runner = QualityCheckRunner(connection)

        # Run individual checks
        results = runner.run_checks(
            table_name="users",
            checks=[
                UniqueCheck(column="user_id"),
                NotNullCheck(column="email"),
            ]
        )

        # Check if all passed
        if results.has_failures:
            raise DataQualityError(f"Quality checks failed for users")
    """

    def __init__(
        self,
        connection: Optional[ibis.BaseBackend] = None,
        fail_fast: bool = False,
    ):
        """
        Initialize quality check runner.

        Args:
            connection: ibis database connection (can be set later)
            fail_fast: Stop on first failure if True
        """
        self.connection = connection
        self.fail_fast = fail_fast

    def run_checks(
        self,
        table_name: str,
        checks: List[QualityCheck],
        schema: Optional[str] = None,
        connection: Optional[ibis.BaseBackend] = None,
    ) -> QualityCheckSummary:
        """
        Run quality checks on a table.

        Args:
            table_name: Name of the table to check
            checks: List of quality checks to run
            schema: Schema containing the table
            connection: Optional connection override

        Returns:
            QualityCheckSummary with all results
        """
        conn = connection or self.connection
        if conn is None:
            raise ValueError("No connection provided")

        start_time = time.time()
        summary = QualityCheckSummary(table_name=table_name, total_checks=len(checks))

        for check in checks:
            try:
                result = check.run(conn, table_name, schema)
                summary.results.append(result)

                # Update counts
                if result.status == QualityCheckStatus.PASSED:
                    summary.passed += 1
                elif result.status == QualityCheckStatus.FAILED:
                    summary.failed += 1

                    # Log based on severity
                    if result.severity == QualityCheckSeverity.ERROR:
                        logger.error(f"Quality check failed: {result.message}")
                    elif result.severity == QualityCheckSeverity.WARN:
                        logger.warning(f"Quality check warning: {result.message}")
                    else:
                        logger.info(f"Quality check info: {result.message}")

                    # Stop on first failure if fail_fast
                    if self.fail_fast and result.severity == QualityCheckSeverity.ERROR:
                        break

                elif result.status == QualityCheckStatus.ERROR:
                    summary.errors += 1
                    logger.error(f"Quality check error: {result.message}")
                elif result.status == QualityCheckStatus.SKIPPED:
                    summary.skipped += 1
                    logger.debug(f"Quality check skipped: {result.message}")

            except Exception as e:
                # Catch any unhandled errors
                logger.error(f"Unexpected error running check {check.check_name}: {e}")
                summary.errors += 1
                summary.results.append(
                    QualityCheckResult(
                        check_name=check.check_name,
                        check_type=check.check_type,
                        status=QualityCheckStatus.ERROR,
                        severity=check.severity,
                        table_name=table_name,
                        message=f"Unexpected error: {str(e)}",
                    )
                )

        summary.duration_seconds = time.time() - start_time

        # Log summary
        if summary.has_failures:
            logger.error(
                f"Quality checks for '{table_name}': "
                f"{summary.passed}/{summary.total_checks} passed, "
                f"{summary.failed} failed ({summary.duration_seconds:.2f}s)"
            )
        else:
            logger.info(
                f"Quality checks for '{table_name}': "
                f"{summary.passed}/{summary.total_checks} passed "
                f"({summary.duration_seconds:.2f}s)"
            )

        return summary

    def run_model_checks(
        self,
        model_name: str,
        model_info: Dict[str, Any],
        schema: Optional[str] = None,
        connection: Optional[ibis.BaseBackend] = None,
    ) -> Optional[QualityCheckSummary]:
        """
        Run quality checks defined in model configuration.

        Args:
            model_name: Name of the model
            model_info: Model configuration dictionary
            schema: Schema containing the model's table
            connection: Optional connection override

        Returns:
            QualityCheckSummary or None if no checks defined
        """
        quality_checks_config = model_info.get("quality_checks", [])
        if not quality_checks_config:
            return None

        checks = self._parse_check_configs(quality_checks_config)
        if not checks:
            return None

        return self.run_checks(
            table_name=model_name,
            checks=checks,
            schema=schema,
            connection=connection,
        )

    def _parse_check_configs(
        self, configs: List[Dict[str, Any]]
    ) -> List[QualityCheck]:
        """
        Parse check configuration dictionaries into QualityCheck objects.

        Args:
            configs: List of check configuration dictionaries

        Returns:
            List of QualityCheck instances
        """
        from interlace.quality.checks import (
            UniqueCheck,
            NotNullCheck,
            AcceptedValuesCheck,
            FreshnessCheck,
            RowCountCheck,
            ExpressionCheck,
        )

        check_types = {
            "unique": UniqueCheck,
            "not_null": NotNullCheck,
            "accepted_values": AcceptedValuesCheck,
            "freshness": FreshnessCheck,
            "row_count": RowCountCheck,
        }

        checks = []
        for config in configs:
            check_type = config.get("type")
            if not check_type:
                logger.warning(f"Quality check config missing 'type': {config}")
                continue

            check_class = check_types.get(check_type)
            if not check_class:
                logger.warning(f"Unknown quality check type: {check_type}")
                continue

            # Parse severity
            severity_str = config.get("severity", "error").lower()
            severity_map = {
                "error": QualityCheckSeverity.ERROR,
                "warn": QualityCheckSeverity.WARN,
                "warning": QualityCheckSeverity.WARN,
                "info": QualityCheckSeverity.INFO,
            }
            severity = severity_map.get(severity_str, QualityCheckSeverity.ERROR)

            # Build check kwargs
            kwargs = {k: v for k, v in config.items() if k not in ("type", "severity")}
            kwargs["severity"] = severity

            try:
                check = check_class(**kwargs)
                checks.append(check)
            except Exception as e:
                logger.warning(f"Error creating {check_type} check: {e}")

        return checks


class DataQualityError(Exception):
    """Raised when data quality checks fail with ERROR severity."""

    def __init__(self, message: str, summary: Optional[QualityCheckSummary] = None):
        super().__init__(message)
        self.summary = summary
