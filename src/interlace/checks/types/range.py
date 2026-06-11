"""
Range check.

Check that column values are within specified bounds.
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

logger = get_logger("interlace.checks.types.range")


class RangeCheck(Check):
    """
    Check that column values fall within a specified range.

    Supports one-sided bounds (min-only or max-only) and inclusive/exclusive.

    Usage:
        RangeCheck(column="price", min_value=0, max_value=10000)
        RangeCheck(column="age", min_value=0, inclusive=True)
    """

    def __init__(
        self,
        column: str,
        min_value: Any = None,
        max_value: Any = None,
        inclusive: bool = True,
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
            raise ValueError("RangeCheck requires a column")
        if min_value is None and max_value is None:
            raise ValueError("RangeCheck requires min_value and/or max_value")

        self.min_value = min_value
        self.max_value = max_value
        self.inclusive = inclusive

    @property
    def check_type(self) -> str:
        return "range"

    @property
    def check_name(self) -> str:
        if self.name:
            return self.name
        parts = [f"range_{self.column}"]
        if self.min_value is not None:
            parts.append(f"min_{self.min_value}")
        if self.max_value is not None:
            parts.append(f"max_{self.max_value}")
        return "_".join(parts)

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: str | None = None,
    ) -> CheckResult:
        """Execute range check."""
        start_time = time.time()

        try:
            table = self._get_table(connection, table_name, schema)
            total_rows = int(table.count().execute())

            if total_rows == 0:
                duration = time.time() - start_time
                return self._make_result(
                    status=CheckStatus.SKIPPED,
                    table_name=table_name,
                    message="Table is empty, skipping range check",
                    total_rows=0,
                    duration=duration,
                )

            col = table[self.column]

            # Build out-of-range filter: NULL or outside bounds
            conditions: list[ibis.expr.types.BooleanValue] = [col.isnull()]

            if self.min_value is not None:
                if self.inclusive:
                    conditions.append(col < self.min_value)
                else:
                    conditions.append(col <= self.min_value)

            if self.max_value is not None:
                if self.inclusive:
                    conditions.append(col > self.max_value)
                else:
                    conditions.append(col >= self.max_value)

            # Combine with OR: any condition means out of range
            out_of_range = conditions[0]
            for cond in conditions[1:]:
                out_of_range = out_of_range | cond

            failed_count = int(table.filter(out_of_range).count().execute())
            duration = time.time() - start_time

            bounds_str = self._bounds_str()

            if failed_count == 0:
                return self._make_result(
                    status=CheckStatus.PASSED,
                    table_name=table_name,
                    message=f"All {total_rows} rows in '{self.column}' are within range {bounds_str}",
                    failed_rows=0,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "column": self.column,
                        "min_value": self.min_value,
                        "max_value": self.max_value,
                        "inclusive": self.inclusive,
                    },
                )
            else:
                return self._make_result(
                    status=CheckStatus.FAILED,
                    table_name=table_name,
                    message=(
                        f"{failed_count} of {total_rows} rows in '{self.column}' "
                        f"are outside range {bounds_str} ({failed_count / total_rows * 100:.1f}%)"
                    ),
                    failed_rows=failed_count,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "column": self.column,
                        "min_value": self.min_value,
                        "max_value": self.max_value,
                        "inclusive": self.inclusive,
                        "out_of_range_count": failed_count,
                        "failure_rate": failed_count / total_rows * 100,
                    },
                )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error running range check: {e}")
            return self._make_result(
                status=CheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running range check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )

    def _bounds_str(self) -> str:
        """Format bounds as string."""
        left = "[" if self.inclusive else "("
        right = "]" if self.inclusive else ")"
        lo = str(self.min_value) if self.min_value is not None else "-inf"
        hi = str(self.max_value) if self.max_value is not None else "inf"
        return f"{left}{lo}, {hi}{right}"
