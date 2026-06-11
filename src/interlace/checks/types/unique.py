"""
Unique constraint check.

Check that column(s) contain only unique values.
"""

import time

import ibis

from interlace.checks.base import (
    Check,
    CheckResult,
    CheckSeverity,
    CheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.checks.types.unique")


class UniqueCheck(Check):
    """
    Check that a column or combination of columns contains unique values.

    Usage:
        # Single column
        UniqueCheck(column="user_id")

        # Composite key
        UniqueCheck(columns=["tenant_id", "user_id"])
    """

    def __init__(
        self,
        column: str | None = None,
        columns: list[str] | None = None,
        severity: CheckSeverity = CheckSeverity.ERROR,
        name: str | None = None,
        description: str | None = None,
    ):
        super().__init__(
            column=column,
            columns=columns,
            severity=severity,
            name=name,
            description=description,
        )

        if not self.columns:
            raise ValueError("UniqueCheck requires at least one column")

    @property
    def check_type(self) -> str:
        return "unique"

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: str | None = None,
    ) -> CheckResult:
        """Execute uniqueness check."""
        start_time = time.time()

        try:
            table = self._get_table(connection, table_name, schema)
            total_rows = int(table.count().execute())

            if len(self.columns) == 1:
                distinct_count = int(table[self.columns[0]].nunique().execute())
            else:
                distinct_count = int(table.select(self.columns).distinct().count().execute())

            duplicate_count = total_rows - distinct_count
            duration = time.time() - start_time

            if duplicate_count == 0:
                return self._make_result(
                    status=CheckStatus.PASSED,
                    table_name=table_name,
                    message=f"All {total_rows} rows have unique values for {self.columns}",
                    failed_rows=0,
                    total_rows=total_rows,
                    duration=duration,
                    details={"distinct_count": distinct_count, "columns": self.columns},
                )
            else:
                return self._make_result(
                    status=CheckStatus.FAILED,
                    table_name=table_name,
                    message=(
                        f"Found {duplicate_count} duplicate values in {self.columns} "
                        f"({distinct_count} distinct out of {total_rows} rows)"
                    ),
                    failed_rows=duplicate_count,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "distinct_count": distinct_count,
                        "duplicate_count": duplicate_count,
                        "columns": self.columns,
                    },
                )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error running unique check: {e}")
            return self._make_result(
                status=CheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running unique check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )
