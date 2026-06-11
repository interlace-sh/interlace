"""
Freshness check.

Check that timestamp column has recent data.
"""

import time
from datetime import UTC, datetime

import ibis

from interlace.checks.base import (
    Check,
    CheckResult,
    CheckSeverity,
    CheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.checks.types.freshness")


class FreshnessCheck(Check):
    """
    Check that a timestamp column has data within a specified age.

    Useful for detecting stale data or broken pipelines.

    Usage:
        FreshnessCheck(column="updated_at", max_age_hours=24)
        FreshnessCheck(column="created_at", max_age_days=7)
    """

    def __init__(
        self,
        column: str,
        max_age_hours: float | None = None,
        max_age_days: float | None = None,
        max_age_minutes: float | None = None,
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
            raise ValueError("FreshnessCheck requires max_age_hours, max_age_days, or max_age_minutes")

        self.max_age_hours = total_hours

    @property
    def check_type(self) -> str:
        return "freshness"

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: str | None = None,
    ) -> CheckResult:
        """Execute freshness check."""
        start_time = time.time()

        try:
            table = self._get_table(connection, table_name, schema)
            total_rows = int(table.count().execute())

            if total_rows == 0:
                duration = time.time() - start_time
                return self._make_result(
                    status=CheckStatus.SKIPPED,
                    table_name=table_name,
                    message="Table is empty, skipping freshness check",
                    total_rows=0,
                    duration=duration,
                )

            max_timestamp = table[self.column].max().execute()

            if max_timestamp is None:
                duration = time.time() - start_time
                return self._make_result(
                    status=CheckStatus.FAILED,
                    table_name=table_name,
                    message=f"Column '{self.column}' has no non-NULL values",
                    total_rows=total_rows,
                    duration=duration,
                )

            # Convert to datetime if needed
            if hasattr(max_timestamp, "to_pydatetime"):
                max_timestamp = max_timestamp.to_pydatetime()
            elif not isinstance(max_timestamp, datetime):
                try:
                    import pandas as pd

                    if isinstance(max_timestamp, pd.Timestamp):
                        max_timestamp = max_timestamp.to_pydatetime()
                except ImportError:
                    pass

            # Calculate age
            now = datetime.now()
            if hasattr(max_timestamp, "tzinfo") and max_timestamp.tzinfo is not None:
                now = datetime.now(UTC)

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
                    status=CheckStatus.PASSED,
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
                    status=CheckStatus.FAILED,
                    table_name=table_name,
                    message=(
                        f"Data is stale. Most recent '{self.column}' is {age_str} old "
                        f"(max allowed: {self.max_age_hours} hours)"
                    ),
                    failed_rows=total_rows,
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
                status=CheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running freshness check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )
