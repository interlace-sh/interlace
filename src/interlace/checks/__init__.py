"""
Data Checks Framework for Interlace.

Provides dbt-style data checks for validation of model outputs.

Usage:
    from interlace.checks import CheckRunner, UniqueCheck, NotNullCheck

    runner = CheckRunner(connection)
    results = runner.run_checks(
        table_name="users",
        checks=[UniqueCheck(column="user_id"), NotNullCheck(column="email")],
    )
"""

from interlace.checks.base import (
    Check,
    CheckResult,
    CheckSeverity,
    CheckStatus,
)
from interlace.checks.decorator import PythonCheck, check
from interlace.checks.runner import CheckRunner, CheckSummary
from interlace.checks.types.accepted_values import AcceptedValuesCheck
from interlace.checks.types.expression import ExpressionCheck
from interlace.checks.types.freshness import FreshnessCheck
from interlace.checks.types.not_null import NotNullCheck
from interlace.checks.types.pattern import PatternCheck
from interlace.checks.types.range import RangeCheck
from interlace.checks.types.relationships import RelationshipsCheck
from interlace.checks.types.row_count import RowCountCheck
from interlace.checks.types.sql import SqlCheck
from interlace.checks.types.unique import UniqueCheck

__all__ = [
    # Base classes
    "Check",
    "CheckResult",
    "CheckSeverity",
    "CheckStatus",
    # Runner
    "CheckRunner",
    "CheckSummary",
    # Decorator
    "check",
    "PythonCheck",
    # Check types
    "AcceptedValuesCheck",
    "ExpressionCheck",
    "FreshnessCheck",
    "NotNullCheck",
    "PatternCheck",
    "RangeCheck",
    "RelationshipsCheck",
    "RowCountCheck",
    "SqlCheck",
    "UniqueCheck",
]
