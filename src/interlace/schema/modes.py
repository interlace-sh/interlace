"""
Schema modes for controlling schema behavior.

This module provides enums for schema-related behavior configuration.
"""

from enum import Enum


class SchemaMode(str, Enum):
    """
    Schema evolution mode controlling how schema changes are handled when
    writing to existing tables.

    Note: This is defined for future use. Currently schema validation uses
    the basic compare_schemas/validate_schema functions in validation.py.

    Modes:
        STRICT: Fail on any schema mismatch (safest for production)
        SAFE: Allow safe changes only - column additions, type widening (default)
        FLEXIBLE: Allow column add/remove, safe type changes
        LENIENT: Allow all changes, coerce types when possible
        IGNORE: Skip schema validation entirely (use with caution)
    """

    STRICT = "strict"
    SAFE = "safe"
    FLEXIBLE = "flexible"
    LENIENT = "lenient"
    IGNORE = "ignore"
