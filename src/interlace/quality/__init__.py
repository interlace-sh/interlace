"""
Data Quality Checks Framework for Interlace.

Phase 3: dbt-style quality checks for data validation.

Usage:
    from interlace.quality import QualityCheckRunner, UniqueCheck, NotNullCheck

    # Run quality checks on a model
    runner = QualityCheckRunner(connection)
    results = runner.run_checks(
        table_name="users",
        checks=[
            UniqueCheck(column="user_id"),
            NotNullCheck(column="email"),
        ]
    )
"""

from interlace.quality.base import (
    QualityCheck,
    QualityCheckResult,
    QualityCheckSeverity,
    QualityCheckStatus,
)
from interlace.quality.checks.accepted_values import AcceptedValuesCheck
from interlace.quality.checks.expression import ExpressionCheck
from interlace.quality.checks.freshness import FreshnessCheck
from interlace.quality.checks.not_null import NotNullCheck
from interlace.quality.checks.row_count import RowCountCheck
from interlace.quality.checks.unique import UniqueCheck
from interlace.quality.runner import QualityCheckRunner

__all__ = [
    # Base classes
    "QualityCheck",
    "QualityCheckResult",
    "QualityCheckSeverity",
    "QualityCheckStatus",
    # Runner
    "QualityCheckRunner",
    # Checks
    "UniqueCheck",
    "NotNullCheck",
    "AcceptedValuesCheck",
    "FreshnessCheck",
    "RowCountCheck",
    "ExpressionCheck",
]
