"""
Row count quality check.

Phase 3: Check that table has expected row count range.
"""

import time
from typing import Optional
import ibis

from interlace.quality.base import (
    QualityCheck,
    QualityCheckResult,
    QualityCheckSeverity,
    QualityCheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.quality.checks.row_count")


class RowCountCheck(QualityCheck):
    """
    Check that a table has a row count within expected bounds.

    Usage:
        # At least 100 rows
        RowCountCheck(min_count=100)

        # Between 1000 and 10000 rows
        RowCountCheck(min_count=1000, max_count=10000)

        # Exactly 0 rows (empty table)
        RowCountCheck(min_count=0, max_count=0)
    """

    def __init__(
        self,
        min_count: Optional[int] = None,
        max_count: Optional[int] = None,
        severity: QualityCheckSeverity = QualityCheckSeverity.ERROR,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """
        Initialize row count check.

        Args:
            min_count: Minimum expected row count (inclusive)
            max_count: Maximum expected row count (inclusive)
            severity: Severity level for failures
            name: Custom check name
            description: Check description
        """
        super().__init__(
            severity=severity,
            name=name,
            description=description,
        )

        if min_count is None and max_count is None:
            raise ValueError("RowCountCheck requires min_count and/or max_count")

        self.min_count = min_count
        self.max_count = max_count

    @property
    def check_type(self) -> str:
        return "row_count"

    @property
    def check_name(self) -> str:
        if self.name:
            return self.name
        parts = ["row_count"]
        if self.min_count is not None:
            parts.append(f"min_{self.min_count}")
        if self.max_count is not None:
            parts.append(f"max_{self.max_count}")
        return "_".join(parts)

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: Optional[str] = None,
    ) -> QualityCheckResult:
        """
        Execute row count check.

        Args:
            connection: ibis connection
            table_name: Table to check
            schema: Schema containing the table

        Returns:
            QualityCheckResult with row count check outcome
        """
        start_time = time.time()

        try:
            table = self._get_table(connection, table_name, schema)

            # Get row count
            row_count = int(table.count().execute())

            duration = time.time() - start_time

            # Check bounds
            below_min = self.min_count is not None and row_count < self.min_count
            above_max = self.max_count is not None and row_count > self.max_count

            if not below_min and not above_max:
                # Passed
                bounds_str = self._bounds_str()
                return self._make_result(
                    status=QualityCheckStatus.PASSED,
                    table_name=table_name,
                    message=f"Row count {row_count} is within expected range {bounds_str}",
                    total_rows=row_count,
                    duration=duration,
                    details={
                        "row_count": row_count,
                        "min_count": self.min_count,
                        "max_count": self.max_count,
                    },
                )
            else:
                # Failed
                if below_min:
                    message = (
                        f"Row count {row_count} is below minimum {self.min_count}"
                    )
                else:
                    message = (
                        f"Row count {row_count} exceeds maximum {self.max_count}"
                    )

                return self._make_result(
                    status=QualityCheckStatus.FAILED,
                    table_name=table_name,
                    message=message,
                    failed_rows=row_count,
                    total_rows=row_count,
                    duration=duration,
                    details={
                        "row_count": row_count,
                        "min_count": self.min_count,
                        "max_count": self.max_count,
                        "below_min": below_min,
                        "above_max": above_max,
                    },
                )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error running row_count check: {e}")
            return self._make_result(
                status=QualityCheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running row_count check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )

    def _bounds_str(self) -> str:
        """Format bounds as string."""
        if self.min_count is not None and self.max_count is not None:
            return f"[{self.min_count}, {self.max_count}]"
        elif self.min_count is not None:
            return f">= {self.min_count}"
        else:
            return f"<= {self.max_count}"
