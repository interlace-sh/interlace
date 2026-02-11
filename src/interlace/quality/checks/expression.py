"""
Custom expression quality check.

Phase 3: Check using custom SQL/ibis expression.
"""

import time
from typing import Any, Callable, Optional
import ibis

from interlace.quality.base import (
    QualityCheck,
    QualityCheckResult,
    QualityCheckSeverity,
    QualityCheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.quality.checks.expression")


class ExpressionCheck(QualityCheck):
    """
    Check using a custom expression.

    Allows defining custom quality checks with ibis expressions or SQL.

    Usage:
        # Using ibis expression
        ExpressionCheck(
            expression=lambda t: t["amount"] > 0,
            name="positive_amount"
        )

        # Check that a computed value equals expected
        ExpressionCheck(
            expression=lambda t: t["start_date"] <= t["end_date"],
            name="valid_date_range"
        )
    """

    def __init__(
        self,
        expression: Callable[[ibis.Table], Any],
        name: str,
        description: Optional[str] = None,
        severity: QualityCheckSeverity = QualityCheckSeverity.ERROR,
        invert: bool = False,
    ):
        """
        Initialize expression check.

        Args:
            expression: Function that takes an ibis.Table and returns a boolean column
                       True means the row passes, False means it fails
            name: Name for this check (required)
            description: Check description
            severity: Severity level for failures
            invert: If True, invert the expression (True means fail)
        """
        super().__init__(
            severity=severity,
            name=name,
            description=description,
        )

        if not callable(expression):
            raise ValueError("ExpressionCheck requires a callable expression")
        if not name:
            raise ValueError("ExpressionCheck requires a name")

        self.expression = expression
        self.invert = invert

    @property
    def check_type(self) -> str:
        return "expression"

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: Optional[str] = None,
    ) -> QualityCheckResult:
        """
        Execute expression check.

        Args:
            connection: ibis connection
            table_name: Table to check
            schema: Schema containing the table

        Returns:
            QualityCheckResult with check outcome
        """
        start_time = time.time()

        try:
            table = self._get_table(connection, table_name, schema)

            # Get total row count
            total_rows = int(table.count().execute())

            if total_rows == 0:
                duration = time.time() - start_time
                return self._make_result(
                    status=QualityCheckStatus.SKIPPED,
                    table_name=table_name,
                    message=f"Table is empty, skipping expression check",
                    total_rows=0,
                    duration=duration,
                )

            # Apply expression
            condition = self.expression(table)

            # If invert, swap pass/fail
            if self.invert:
                condition = ~condition

            # Count rows that fail (where expression is False)
            failed_count = int(table.filter(~condition).count().execute())

            duration = time.time() - start_time

            if failed_count == 0:
                return self._make_result(
                    status=QualityCheckStatus.PASSED,
                    table_name=table_name,
                    message=f"All {total_rows} rows pass expression check '{self.name}'",
                    failed_rows=0,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "expression_name": self.name,
                        "inverted": self.invert,
                    },
                )
            else:
                return self._make_result(
                    status=QualityCheckStatus.FAILED,
                    table_name=table_name,
                    message=(
                        f"{failed_count} of {total_rows} rows fail expression check '{self.name}' "
                        f"({failed_count/total_rows*100:.1f}%)"
                    ),
                    failed_rows=failed_count,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "expression_name": self.name,
                        "inverted": self.invert,
                        "failure_rate": failed_count / total_rows * 100,
                    },
                )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error running expression check '{self.name}': {e}")
            return self._make_result(
                status=QualityCheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running expression check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )
