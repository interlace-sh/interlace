"""
Accepted values quality check.

Phase 3: Check that column values are within an expected set.
"""

import time
from typing import Any, List, Optional, Set
import ibis

from interlace.quality.base import (
    QualityCheck,
    QualityCheckResult,
    QualityCheckSeverity,
    QualityCheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.quality.checks.accepted_values")


class AcceptedValuesCheck(QualityCheck):
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
        values: List[Any],
        severity: QualityCheckSeverity = QualityCheckSeverity.ERROR,
        name: Optional[str] = None,
        description: Optional[str] = None,
        quote_values: bool = True,
    ):
        """
        Initialize accepted values check.

        Args:
            column: Column to check
            values: List of accepted values
            severity: Severity level for failures
            name: Custom check name
            description: Check description
            quote_values: Whether to quote string values in messages
        """
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
        schema: Optional[str] = None,
    ) -> QualityCheckResult:
        """
        Execute accepted values check.

        Finds values not in the accepted set.

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

            # Filter for rows with unaccepted values
            invalid_filter = ~table[self.column].isin(self.values)
            invalid_count = int(table.filter(invalid_filter).count().execute())

            duration = time.time() - start_time

            if invalid_count == 0:
                return self._make_result(
                    status=QualityCheckStatus.PASSED,
                    table_name=table_name,
                    message=(
                        f"All {total_rows} rows have accepted values in '{self.column}'"
                    ),
                    failed_rows=0,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "column": self.column,
                        "accepted_values": self.values,
                    },
                )
            else:
                # Get sample of invalid values for debugging
                invalid_values = []
                try:
                    sample = (
                        table.filter(invalid_filter)
                        .select(self.column)
                        .distinct()
                        .limit(10)
                        .execute()
                    )
                    invalid_values = sample[self.column].tolist()
                except Exception:
                    pass

                return self._make_result(
                    status=QualityCheckStatus.FAILED,
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
                status=QualityCheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running accepted_values check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )
