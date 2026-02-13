"""
Schema validation and evolution module.

Provides schema comparison and validation capabilities.
"""

from interlace.schema.evolution import (
    apply_schema_changes,
    get_schema_changes,
    should_apply_schema_changes,
)
from interlace.schema.modes import SchemaMode
from interlace.schema.tracking import (
    get_current_schema_version,
    get_schema_history,
    track_schema_version,
)
from interlace.schema.validation import (
    SchemaComparisonResult,
    compare_schemas,
    validate_schema,
)

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
