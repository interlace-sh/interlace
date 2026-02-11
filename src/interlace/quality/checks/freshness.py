"""
Freshness quality check.

Phase 3: Check that timestamp column has recent data.
"""

import time
from datetime import datetime, timedelta
from typing import Optional
import ibis

from interlace.quality.base import (
    QualityCheck,
    QualityCheckResult,
    QualityCheckSeverity,
    QualityCheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.quality.checks.freshness")


class FreshnessCheck(QualityCheck):
    """
    Check that a timestamp column has data within a specified age.

    Useful for detecting stale data or broken pipelines.

    Usage:
        # Data should be no older than 24 hours
        FreshnessCheck(column="updated_at", max_age_hours=24)

        # Data should be no older than 7 days
        FreshnessCheck(column="created_at", max_age_days=7)
    """

    def __init__(
        self,
        column: str,
        max_age_hours: Optional[float] = None,
        max_age_days: Optional[float] = None,
        max_age_minutes: Optional[float] = None,
        severity: QualityCheckSeverity = QualityCheckSeverity.ERROR,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """
        Initialize freshness check.

        Args:
            column: Timestamp column to check
            max_age_hours: Maximum age in hours
            max_age_days: Maximum age in days
            max_age_minutes: Maximum age in minutes
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
            raise ValueError("FreshnessCheck requires a column")

        # Calculate max age in hours (normalize all inputs)
        total_hours = 0.0
        if max_age_days:
            total_hours += max_age_days * 24
        if max_age_hours:
            total_hours += max_age_hours
        if max_age_minutes:
            total_hours += max_age_minutes / 60

        if total_hours <= 0:
            raise ValueError(
                "FreshnessCheck requires max_age_hours, max_age_days, or max_age_minutes"
            )

        self.max_age_hours = total_hours

    @property
    def check_type(self) -> str:
        return "freshness"

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: Optional[str] = None,
    ) -> QualityCheckResult:
        """
        Execute freshness check.

        Compares the most recent timestamp to the current time.

        Args:
            connection: ibis connection
            table_name: Table to check
            schema: Schema containing the table

        Returns:
            QualityCheckResult with freshness check outcome
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
                    message=f"Table is empty, skipping freshness check",
                    total_rows=0,
                    duration=duration,
                )

            # Get the most recent timestamp
            max_timestamp = table[self.column].max().execute()

            if max_timestamp is None:
                duration = time.time() - start_time
                return self._make_result(
                    status=QualityCheckStatus.FAILED,
                    table_name=table_name,
                    message=f"Column '{self.column}' has no non-NULL values",
                    total_rows=total_rows,
                    duration=duration,
                )

            # Convert to datetime if needed
            if hasattr(max_timestamp, "to_pydatetime"):
                max_timestamp = max_timestamp.to_pydatetime()
            elif not isinstance(max_timestamp, datetime):
                # Try pandas Timestamp
                try:
                    import pandas as pd
                    if isinstance(max_timestamp, pd.Timestamp):
                        max_timestamp = max_timestamp.to_pydatetime()
                except ImportError:
                    pass

            # Calculate age
            now = datetime.now()
            if hasattr(max_timestamp, "tzinfo") and max_timestamp.tzinfo is not None:
                # Use timezone-aware now
                from datetime import timezone
                now = datetime.now(timezone.utc)

            age = now - max_timestamp
            age_hours = age.total_seconds() / 3600

            duration = time.time() - start_time

            # Format age for display
            if age_hours < 1:
                age_str = f"{age.total_seconds() / 60:.1f} minutes"
            elif age_hours < 24:
                age_str = f"{age_hours:.1f} hours"
            else:
                age_str = f"{age_hours / 24:.1f} days"

            if age_hours <= self.max_age_hours:
                return self._make_result(
                    status=QualityCheckStatus.PASSED,
                    table_name=table_name,
                    message=(
                        f"Data is fresh. Most recent '{self.column}' is {age_str} old "
                        f"(max allowed: {self.max_age_hours} hours)"
                    ),
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "column": self.column,
                        "max_timestamp": str(max_timestamp),
                        "age_hours": age_hours,
                        "max_age_hours": self.max_age_hours,
                    },
                )
            else:
                return self._make_result(
                    status=QualityCheckStatus.FAILED,
                    table_name=table_name,
                    message=(
                        f"Data is stale. Most recent '{self.column}' is {age_str} old "
                        f"(max allowed: {self.max_age_hours} hours)"
                    ),
                    failed_rows=total_rows,  # All rows are "stale"
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "column": self.column,
                        "max_timestamp": str(max_timestamp),
                        "age_hours": age_hours,
                        "max_age_hours": self.max_age_hours,
                        "exceeded_by_hours": age_hours - self.max_age_hours,
                    },
                )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error running freshness check: {e}")
            return self._make_result(
                status=QualityCheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running freshness check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )
