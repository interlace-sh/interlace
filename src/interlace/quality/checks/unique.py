"""
Unique constraint quality check.

Phase 3: Check that column(s) contain only unique values.
"""

import time
from typing import List, Optional
import ibis

from interlace.quality.base import (
    QualityCheck,
    QualityCheckResult,
    QualityCheckSeverity,
    QualityCheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.quality.checks.unique")


class UniqueCheck(QualityCheck):
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
        column: Optional[str] = None,
        columns: Optional[List[str]] = None,
        severity: QualityCheckSeverity = QualityCheckSeverity.ERROR,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """
        Initialize unique check.

        Args:
            column: Single column to check for uniqueness
            columns: Multiple columns for composite uniqueness
            severity: Severity level for failures
            name: Custom check name
            description: Check description
        """
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
        schema: Optional[str] = None,
    ) -> QualityCheckResult:
        """
        Execute uniqueness check.

        Counts duplicate values and reports failures.

        Args:
            connection: ibis connection
            table_name: Table to check
            schema: Schema containing the table

        Returns:
            QualityCheckResult with uniqueness check outcome
        """
        start_time = time.time()

        try:
            table = self._get_table(connection, table_name, schema)

            # Get total row count
            total_rows = int(table.count().execute())

            # Count distinct values for the column(s)
            if len(self.columns) == 1:
                distinct_count = int(table[self.columns[0]].nunique().execute())
            else:
                # For composite keys, count distinct combinations
                distinct_count = int(
                    table.select(self.columns).distinct().count().execute()
                )

            # Calculate duplicates
            duplicate_count = total_rows - distinct_count

            duration = time.time() - start_time

            if duplicate_count == 0:
                return self._make_result(
                    status=QualityCheckStatus.PASSED,
                    table_name=table_name,
                    message=f"All {total_rows} rows have unique values for {self.columns}",
                    failed_rows=0,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "distinct_count": distinct_count,
                        "columns": self.columns,
                    },
                )
            else:
                return self._make_result(
                    status=QualityCheckStatus.FAILED,
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
                status=QualityCheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running unique check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )
