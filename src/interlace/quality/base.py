"""
Base classes for data quality checks.

Phase 3: Abstract base class for quality checks with common interface.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import ibis

from interlace.utils.logging import get_logger

logger = get_logger("interlace.quality.base")


class QualityCheckSeverity(Enum):
    """Severity levels for quality check failures."""

    ERROR = "error"  # Fail the pipeline
    WARN = "warn"  # Log warning, continue pipeline
    INFO = "info"  # Informational only


class QualityCheckStatus(Enum):
    """Status of a quality check execution."""

    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"  # Check itself errored (not data quality issue)


@dataclass
class QualityCheckResult:
    """
    Result of a quality check execution.

    Attributes:
        check_name: Name of the check
        check_type: Type of check (unique, not_null, etc.)
        status: Check status (passed, failed, skipped, error)
        severity: Check severity (error, warn, info)
        table_name: Table that was checked
        column_name: Column that was checked (if applicable)
        message: Human-readable result message
        details: Additional check-specific details
        failed_rows: Number of rows that failed the check
        total_rows: Total number of rows checked
        executed_at: Timestamp when check was executed
        duration_seconds: How long the check took
        sql_query: SQL query used for the check (for debugging)
    """

    check_name: str
    check_type: str
    status: QualityCheckStatus
    severity: QualityCheckSeverity
    table_name: str
    column_name: str | None = None
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    failed_rows: int = 0
    total_rows: int = 0
    executed_at: datetime = field(default_factory=datetime.now)
    duration_seconds: float = 0.0
    sql_query: str | None = None

    @property
    def passed(self) -> bool:
        """Check if the quality check passed."""
        return self.status == QualityCheckStatus.PASSED

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate as percentage."""
        if self.total_rows == 0:
            return 0.0
        return (self.failed_rows / self.total_rows) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "check_name": self.check_name,
            "check_type": self.check_type,
            "status": self.status.value,
            "severity": self.severity.value,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "message": self.message,
            "details": self.details,
            "failed_rows": self.failed_rows,
            "total_rows": self.total_rows,
            "executed_at": self.executed_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "failure_rate": self.failure_rate,
        }


class QualityCheck(ABC):
    """
    Abstract base class for quality checks.

    Subclasses implement specific check types (unique, not_null, etc.).

    Usage:
        class UniqueCheck(QualityCheck):
            def run(self, connection, table_name, schema=None):
                # Implement uniqueness check
                ...
    """

    def __init__(
        self,
        column: str | None = None,
        columns: list[str] | None = None,
        severity: QualityCheckSeverity = QualityCheckSeverity.ERROR,
        name: str | None = None,
        description: str | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize quality check.

        Args:
            column: Single column to check
            columns: Multiple columns to check (for multi-column checks)
            severity: Severity level for failures
            name: Custom name for the check
            description: Description of what the check validates
            **kwargs: Additional check-specific parameters
        """
        self.column = column
        self.columns = columns or ([column] if column else [])
        self.severity = severity
        self.name = name
        self.description = description
        self.kwargs = kwargs

    @property
    @abstractmethod
    def check_type(self) -> str:
        """Return the type of this check (e.g., 'unique', 'not_null')."""
        pass

    @property
    def check_name(self) -> str:
        """Return the name of this check instance."""
        if self.name:
            return self.name
        if self.column:
            return f"{self.check_type}_{self.column}"
        if self.columns:
            return f"{self.check_type}_{'_'.join(self.columns)}"
        return self.check_type

    @abstractmethod
    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: str | None = None,
    ) -> QualityCheckResult:
        """
        Execute the quality check.

        Args:
            connection: ibis database connection
            table_name: Name of the table to check
            schema: Schema/database containing the table

        Returns:
            QualityCheckResult with check outcome
        """
        pass

    def _get_table(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: str | None = None,
    ) -> ibis.Table:
        """
        Get ibis table reference.

        Args:
            connection: ibis connection
            table_name: Table name
            schema: Schema name

        Returns:
            ibis.Table reference
        """
        if schema:
            return connection.table(table_name, database=schema)
        return connection.table(table_name)

    def _make_result(
        self,
        status: QualityCheckStatus,
        table_name: str,
        message: str,
        failed_rows: int = 0,
        total_rows: int = 0,
        duration: float = 0.0,
        details: dict[str, Any] | None = None,
        sql_query: str | None = None,
    ) -> QualityCheckResult:
        """
        Create a QualityCheckResult.

        Helper method to create consistent result objects.
        """
        return QualityCheckResult(
            check_name=self.check_name,
            check_type=self.check_type,
            status=status,
            severity=self.severity,
            table_name=table_name,
            column_name=self.column,
            message=message,
            details=details or {},
            failed_rows=failed_rows,
            total_rows=total_rows,
            duration_seconds=duration,
            sql_query=sql_query,
        )
