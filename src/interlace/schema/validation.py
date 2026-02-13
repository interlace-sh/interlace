"""
Schema validation and comparison.

Compares schemas and validates compatibility.
Supports SchemaMode for controlling validation strictness.
"""

from dataclasses import dataclass
from typing import Any

import ibis

from interlace.schema.modes import SchemaMode
from interlace.utils.logging import get_logger

logger = get_logger("interlace.schema.validation")


@dataclass
class SchemaComparisonResult:
    """Result of schema comparison."""

    added_columns: dict[str, Any]  # column_name -> ibis type
    removed_columns: list[str]  # column names
    type_changes: list[tuple[str, Any, Any]]  # (column_name, old_type, new_type)
    compatible: bool
    warnings: list[str]
    errors: list[str]


def compare_schemas(
    existing_schema: ibis.Schema,
    new_schema: ibis.Schema,
    fields_schema: ibis.Schema | None = None,
    schema_mode: SchemaMode | None = None,
) -> SchemaComparisonResult:
    """
    Compare two schemas and identify differences.

    If fields_schema is provided, only validates/ensures those specified fields exist
    (and are of the correct type if type is specified). Ignores automatic additions/removals
    of columns NOT in fields_schema, allowing other columns to exist flexibly.

    The ``schema_mode`` controls how strictly differences are treated:
    - **STRICT**: Any change (add, remove, type change) is an error
    - **SAFE** (default): Column additions and safe type widening OK; removals warn
    - **FLEXIBLE**: Column add/remove OK; safe type changes OK; unsafe type warn
    - **LENIENT**: All changes OK with coercion; only truly incompatible types error
    - **IGNORE**: Skip validation entirely (always compatible)

    Args:
        existing_schema: Existing table schema
        new_schema: New schema from model output
        fields_schema: Optional explicit fields schema from model decorator.
                      If specified, only these fields are validated/ensured.
        schema_mode: Schema evolution mode (default: SAFE)

    Returns:
        SchemaComparisonResult with differences
    """
    if schema_mode is None:
        schema_mode = SchemaMode.SAFE

    # IGNORE mode: skip all validation
    if schema_mode == SchemaMode.IGNORE:
        return SchemaComparisonResult(
            added_columns={},
            removed_columns=[],
            type_changes=[],
            compatible=True,
            warnings=[],
            errors=[],
        )

    existing_cols = {col: existing_schema[col] for col in existing_schema}
    new_cols = {col: new_schema[col] for col in new_schema}

    # If fields_schema is specified, only validate/ensure those fields exist
    if fields_schema is not None:
        specified_fields = {col: fields_schema[col] for col in fields_schema}

        # Merge specified fields with new_schema to ensure they exist
        from interlace.utils.schema_utils import merge_schemas

        new_schema = merge_schemas(new_schema, fields_schema)

        # Only track additions/removals/type changes for specified fields
        added_columns = {name: dtype for name, dtype in specified_fields.items() if name not in existing_cols}
        removed_columns = []  # Don't track removals when fields_schema is specified

        type_changes = []
        for name in specified_fields:
            if name in existing_cols:
                if existing_cols[name] != specified_fields[name]:
                    type_changes.append((name, existing_cols[name], specified_fields[name]))
    else:
        # Normal comparison - track all additions/removals
        added_columns = {name: dtype for name, dtype in new_cols.items() if name not in existing_cols}
        removed_columns = [name for name in existing_cols if name not in new_cols]

        type_changes = []
        for name in existing_cols:
            if name in new_cols and existing_cols[name] != new_cols[name]:
                type_changes.append((name, existing_cols[name], new_cols[name]))

    warnings = []
    errors = []

    # --- Mode-based validation ---

    # Handle added columns
    if added_columns:
        if schema_mode == SchemaMode.STRICT:
            for col_name in added_columns:
                errors.append(f"Column added: {col_name} (strict mode rejects additions)")
        # SAFE, FLEXIBLE, LENIENT: additions are fine

    # Handle removed columns
    if removed_columns:
        if schema_mode == SchemaMode.STRICT:
            for col_name in removed_columns:
                errors.append(f"Column removed: {col_name} (strict mode rejects removals)")
        elif schema_mode == SchemaMode.SAFE:
            for col_name in removed_columns:
                warnings.append(f"Column removed: {col_name} (will be preserved with NULL)")
        # FLEXIBLE, LENIENT: removals are OK (drop the columns)

    # Handle type changes
    for col_name, old_type, new_type in type_changes:
        is_safe = _is_safe_type_cast(old_type, new_type)

        if schema_mode == SchemaMode.STRICT:
            errors.append(
                f"Type change for {col_name}: {old_type} → {new_type} " "(strict mode rejects all type changes)"
            )
        elif schema_mode == SchemaMode.SAFE:
            if not is_safe:
                errors.append(
                    f"Unsafe type change for {col_name}: {old_type} → {new_type}. " "Use migration for unsafe casts."
                )
        elif schema_mode == SchemaMode.FLEXIBLE:
            if not is_safe:
                warnings.append(
                    f"Unsafe type change for {col_name}: {old_type} → {new_type}. " "Will attempt coercion."
                )
        elif schema_mode == SchemaMode.LENIENT:
            if not is_safe:
                warnings.append(f"Type coercion for {col_name}: {old_type} → {new_type}")
        # IGNORE already handled above

    compatible = len(errors) == 0

    return SchemaComparisonResult(
        added_columns=added_columns,
        removed_columns=removed_columns,
        type_changes=type_changes,
        compatible=compatible,
        warnings=warnings,
        errors=errors,
    )


def validate_schema(
    existing_schema: ibis.Schema | None,
    new_schema: ibis.Schema,
    fields_schema: ibis.Schema | None = None,
    schema_mode: SchemaMode | None = None,
) -> SchemaComparisonResult:
    """
    Validate schema compatibility.

    Args:
        existing_schema: Existing table schema (None if table doesn't exist)
        new_schema: New schema from model output
        fields_schema: Optional explicit fields schema from model decorator
        schema_mode: Schema evolution mode (default: SAFE)

    Returns:
        SchemaComparisonResult with validation result
    """
    if existing_schema is None:
        # Table doesn't exist, schema is compatible
        return SchemaComparisonResult(
            added_columns={},
            removed_columns=[],
            type_changes=[],
            compatible=True,
            warnings=[],
            errors=[],
        )

    return compare_schemas(existing_schema, new_schema, fields_schema, schema_mode)


def _is_safe_type_cast(old_type: Any, new_type: Any) -> bool:
    """
    Check if type cast is safe.

    Safe casts:
    - INTEGER → BIGINT
    - FLOAT → DOUBLE
    - VARCHAR → TEXT
    - Same type family (e.g., int32 → int64)

    Args:
        old_type: Old ibis type
        new_type: New ibis type

    Returns:
        True if cast is safe, False otherwise
    """
    # Convert to string for comparison
    old_str = str(old_type).upper()
    new_str = str(new_type).upper()

    # Same type is always safe
    if old_str == new_str:
        return True

    # Safe widening casts
    safe_casts = [
        ("INTEGER", "BIGINT"),
        ("INT32", "INT64"),
        ("INT16", "INT32"),
        ("INT16", "INT64"),
        ("INT8", "INT16"),
        ("INT8", "INT32"),
        ("INT8", "INT64"),
        ("FLOAT", "DOUBLE"),
        ("FLOAT32", "FLOAT64"),
        ("VARCHAR", "TEXT"),
        ("STRING", "TEXT"),
    ]

    for old, new in safe_casts:
        if old in old_str and new in new_str:
            return True

    return False
