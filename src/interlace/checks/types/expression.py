"""
Custom expression check.

Check using custom ibis expression.
"""

import time
from collections.abc import Callable
from typing import Any

import ibis

from interlace.checks.base import (
    Check,
    CheckResult,
    CheckSeverity,
    CheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.checks.types.expression")


class ExpressionCheck(Check):
    """
    Check using a custom expression.

    Allows defining custom checks with ibis expressions.

    Usage:
        ExpressionCheck(
            expression=lambda t: t["amount"] > 0,
            name="positive_amount"
        )
    """

    def __init__(
        self,
        expression: Callable[[ibis.Table], Any],
        name: str,
        description: str | None = None,
        severity: CheckSeverity = CheckSeverity.ERROR,
        invert: bool = False,
    ):
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
        schema: str | None = None,
    ) -> CheckResult:
        """Execute expression check."""
        start_time = time.time()

        try:
            table = self._get_table(connection, table_name, schema)
            total_rows = int(table.count().execute())

            if total_rows == 0:
                duration = time.time() - start_time
                return self._make_result(
                    status=CheckStatus.SKIPPED,
                    table_name=table_name,
                    message="Table is empty, skipping expression check",
                    total_rows=0,
                    duration=duration,
                )

            condition = self.expression(table)
            if self.invert:
                condition = ~condition

            failed_count = int(table.filter(~condition).count().execute())
            duration = time.time() - start_time

            if failed_count == 0:
                return self._make_result(
                    status=CheckStatus.PASSED,
                    table_name=table_name,
                    message=f"All {total_rows} rows pass expression check '{self.name}'",
                    failed_rows=0,
                    total_rows=total_rows,
                    duration=duration,
                    details={"expression_name": self.name, "inverted": self.invert},
                )
            else:
                return self._make_result(
                    status=CheckStatus.FAILED,
                    table_name=table_name,
                    message=(
                        f"{failed_count} of {total_rows} rows fail expression check '{self.name}' "
                        f"({failed_count / total_rows * 100:.1f}%)"
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
                status=CheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running expression check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )
