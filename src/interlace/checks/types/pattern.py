"""
Pattern (regex) check.

Check that column values match a regular expression pattern.
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

logger = get_logger("interlace.checks.types.pattern")


class PatternCheck(Check):
    """
    Check that column values match a regex pattern.

    Usage:
        PatternCheck(column="email", pattern=r"^[^@]+@[^@]+\\.[^@]+$")
        PatternCheck(column="phone", pattern=r"^\\+?[0-9]{10,15}$")
    """

    def __init__(
        self,
        column: str,
        pattern: str,
        severity: CheckSeverity = CheckSeverity.ERROR,
        name: str | None = None,
        description: str | None = None,
    ):
        super().__init__(
            column=column,
            severity=severity,
            name=name,
            description=description,
        )

        if not column:
            raise ValueError("PatternCheck requires a column")
        if not pattern:
            raise ValueError("PatternCheck requires a pattern")

        self.pattern = pattern

    @property
    def check_type(self) -> str:
        return "pattern"

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: str | None = None,
    ) -> CheckResult:
        """Execute pattern check."""
        start_time = time.time()

        try:
            table = self._get_table(connection, table_name, schema)
            total_rows = int(table.count().execute())

            if total_rows == 0:
                duration = time.time() - start_time
                return self._make_result(
                    status=CheckStatus.SKIPPED,
                    table_name=table_name,
                    message="Table is empty, skipping pattern check",
                    total_rows=0,
                    duration=duration,
                )

            col = table[self.column]
            # NULLs are treated as failures
            non_matching = table.filter(col.isnull() | ~col.re_search(self.pattern))
            failed_count = int(non_matching.count().execute())

            duration = time.time() - start_time

            if failed_count == 0:
                return self._make_result(
                    status=CheckStatus.PASSED,
                    table_name=table_name,
                    message=(f"All {total_rows} rows in '{self.column}' match pattern '{self.pattern}'"),
                    failed_rows=0,
                    total_rows=total_rows,
                    duration=duration,
                    details={"column": self.column, "pattern": self.pattern},
                )
            else:
                # Get sample of non-matching values
                sample_values: list[object] = []
                try:
                    sample = non_matching.select(self.column).limit(10).execute()
                    sample_values = sample[self.column].tolist()
                except Exception:
                    pass

                return self._make_result(
                    status=CheckStatus.FAILED,
                    table_name=table_name,
                    message=(
                        f"{failed_count} of {total_rows} rows in '{self.column}' "
                        f"do not match pattern '{self.pattern}' "
                        f"({failed_count / total_rows * 100:.1f}%)"
                    ),
                    failed_rows=failed_count,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "column": self.column,
                        "pattern": self.pattern,
                        "non_matching_sample": sample_values[:10],
                        "failure_rate": failed_count / total_rows * 100,
                    },
                )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error running pattern check: {e}")
            return self._make_result(
                status=CheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running pattern check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )
