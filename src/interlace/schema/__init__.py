"""
Schema validation and evolution module.

Provides schema comparison and validation capabilities.
"""

from interlace.schema.validation import (
    validate_schema,
    compare_schemas,
    SchemaComparisonResult,
)
from interlace.schema.evolution import (
    apply_schema_changes,
    should_apply_schema_changes,
    get_schema_changes,
)
from interlace.schema.tracking import (
    track_schema_version,
    get_current_schema_version,
    get_schema_history,
)
from interlace.schema.modes import SchemaMode

__all__ = [
    # Validation
    "validate_schema",
    "compare_schemas",
    "SchemaComparisonResult",
    # Evolution
    "apply_schema_changes",
    "should_apply_schema_changes",
    "get_schema_changes",
    # Tracking
    "track_schema_version",
    "get_current_schema_version",
    "get_schema_history",
    # Modes
    "SchemaMode",
]
