"""
Accepted values check.

Check that column values are within an expected set.
"""

import time
from typing import Any

import ibis

from interlace.checks.base import (
    Check,
    CheckResult,
    CheckSeverity,
    CheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.checks.types.accepted_values")


class AcceptedValuesCheck(Check):
    """
    Check that column values are within a specified set of accepted values.

    Usage:
        AcceptedValuesCheck(
            column="status",
            values=["active", "inactive", "pending"]
        )
    """

    def __init__(
        self,
        column: str,
        values: list[Any],
        severity: CheckSeverity = CheckSeverity.ERROR,
        name: str | None = None,
        description: str | None = None,
        quote_values: bool = True,
    ):
        super().__init__(
            column=column,
            severity=severity,
            name=name,
            description=description,
        )

        if not column:
            raise ValueError("AcceptedValuesCheck requires a column")
        if not values:
            raise ValueError("AcceptedValuesCheck requires at least one accepted value")

        self.values = values
        self.quote_values = quote_values

    @property
    def check_type(self) -> str:
        return "accepted_values"

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: str | None = None,
    ) -> CheckResult:
        """Execute accepted values check."""
        start_time = time.time()

        try:
            table = self._get_table(connection, table_name, schema)
            total_rows = int(table.count().execute())

            invalid_filter = ~table[self.column].isin(self.values)
            invalid_count = int(table.filter(invalid_filter).count().execute())

            duration = time.time() - start_time

            if invalid_count == 0:
                return self._make_result(
                    status=CheckStatus.PASSED,
                    table_name=table_name,
                    message=f"All {total_rows} rows have accepted values in '{self.column}'",
                    failed_rows=0,
                    total_rows=total_rows,
                    duration=duration,
                    details={"column": self.column, "accepted_values": self.values},
                )
            else:
                invalid_values: list[Any] = []
                try:
                    sample = table.filter(invalid_filter).select(self.column).distinct().limit(10).execute()
                    invalid_values = sample[self.column].tolist()
                except Exception:
                    pass

                return self._make_result(
                    status=CheckStatus.FAILED,
                    table_name=table_name,
                    message=(
                        f"Found {invalid_count} rows with values not in accepted set "
                        f"for '{self.column}'. Invalid values: {invalid_values[:5]}"
                    ),
                    failed_rows=invalid_count,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "column": self.column,
                        "accepted_values": self.values,
                        "invalid_values_sample": invalid_values[:10],
                        "invalid_count": invalid_count,
                    },
                )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error running accepted_values check: {e}")
            return self._make_result(
                status=CheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running accepted_values check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )
