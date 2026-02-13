"""
Not NULL quality check.

Phase 3: Check that column(s) do not contain NULL values.
"""

import time

import ibis

from interlace.quality.base import (
    QualityCheck,
    QualityCheckResult,
    QualityCheckSeverity,
    QualityCheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.quality.checks.not_null")


class NotNullCheck(QualityCheck):
    """
    Check that a column does not contain NULL values.

    Usage:
        NotNullCheck(column="email")
        NotNullCheck(column="user_id", severity=QualityCheckSeverity.WARN)
    """

    def __init__(
        self,
        column: str,
        severity: QualityCheckSeverity = QualityCheckSeverity.ERROR,
        name: str | None = None,
        description: str | None = None,
    ):
        """
        Initialize not null check.

        Args:
            column: Column to check for NULL values
            severity: Severity level for failures
            name: Custom check name
            description: Check description
        """
        super().__init__(
            column=column,
            severity=severity,
            name=name,
            description=description,
        )

        if not column:
            raise ValueError("NotNullCheck requires a column")

    @property
    def check_type(self) -> str:
        return "not_null"

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: str | None = None,
    ) -> QualityCheckResult:
        """
        Execute not null check.

        Counts NULL values in the specified column.

        Args:
            connection: ibis connection
            table_name: Table to check
            schema: Schema containing the table

        Returns:
            QualityCheckResult with null check outcome
        """
        start_time = time.time()

        try:
            table = self._get_table(connection, table_name, schema)

            # Get total row count
            total_rows = int(table.count().execute())

            # Count null values
            null_count = int(table.filter(table[self.column].isnull()).count().execute())

            duration = time.time() - start_time

            if null_count == 0:
                return self._make_result(
                    status=QualityCheckStatus.PASSED,
                    table_name=table_name,
                    message=f"Column '{self.column}' has no NULL values ({total_rows} rows checked)",
                    failed_rows=0,
                    total_rows=total_rows,
                    duration=duration,
                    details={"column": self.column},
                )
            else:
                return self._make_result(
                    status=QualityCheckStatus.FAILED,
                    table_name=table_name,
                    message=(
                        f"Column '{self.column}' has {null_count} NULL values "
                        f"({null_count}/{total_rows} = {null_count/total_rows*100:.1f}%)"
                    ),
                    failed_rows=null_count,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "column": self.column,
                        "null_count": null_count,
                        "null_percentage": null_count / total_rows * 100 if total_rows > 0 else 0,
                    },
                )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error running not_null check: {e}")
            return self._make_result(
                status=QualityCheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running not_null check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )
