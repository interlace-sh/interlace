"""
Schema evolution logic.

Handles applying schema changes to tables.
"""

from typing import Any

import ibis

from interlace.core.context import _execute_sql_internal
from interlace.schema.validation import _is_safe_type_cast, compare_schemas
from interlace.utils.logging import get_logger

logger = get_logger("interlace.schema.evolution")


def get_schema_changes(
    existing_schema: ibis.Schema,
    new_schema: ibis.Schema,
    fields_schema: ibis.Schema | None = None,
) -> dict[str, Any]:
    """
    Get schema changes that need to be applied.

    Args:
        existing_schema: Existing table schema
        new_schema: New schema from model output
        fields_schema: Optional explicit fields schema from model decorator (adds/replaces fields)

    Returns:
        Dictionary with schema changes to apply
    """
    comparison = compare_schemas(existing_schema, new_schema, fields_schema)

    return {
        "added_columns": comparison.added_columns,
        "removed_columns": comparison.removed_columns,
        "type_changes": comparison.type_changes,
        "warnings": comparison.warnings,
        "errors": comparison.errors,
    }


def should_apply_schema_changes(
    existing_schema: ibis.Schema,
    new_schema: ibis.Schema,
    fields_schema: ibis.Schema | None = None,
) -> bool:
    """
    Check if schema changes should be applied.

    Args:
        existing_schema: Existing table schema
        new_schema: New schema from model output
        fields_schema: Optional explicit fields schema from model decorator (adds/replaces fields)

    Returns:
        True if changes should be applied, False otherwise
    """
    comparison = compare_schemas(existing_schema, new_schema, fields_schema)

    # Don't apply if there are errors
    if comparison.errors:
        return False

    # Apply if there are added columns or safe type changes
    # Removed columns are handled by safe mode (preserve columns, NULL for new data)
    return len(comparison.added_columns) > 0 or len(comparison.type_changes) > 0


def apply_schema_changes(
    connection: ibis.BaseBackend,
    model_name: str,
    schema: str,
    existing_schema: ibis.Schema,
    new_schema: ibis.Schema,
    fields_schema: ibis.Schema | None = None,
) -> int:
    """
    Apply schema changes to table.

    Safe column removal: Columns are preserved (not dropped), new data will have NULL
    for missing columns. Breaking changes require migrations.

    Args:
        connection: Database connection
        model_name: Model name (table name)
        schema: Schema name
        existing_schema: Existing table schema
        new_schema: New schema from model output
        fields_schema: Optional explicit fields schema from model decorator (adds/replaces fields)

    Returns:
        Number of schema changes applied
    """
    comparison = compare_schemas(existing_schema, new_schema, fields_schema)

    if comparison.errors:
        raise ValueError(f"Schema validation errors: {', '.join(comparison.errors)}")

    qualified_name = f"{schema}.{model_name}"
    alter_statements = []

    # Add new columns (fields_schema adds/replaces, so new columns are still added)
    for col_name, col_type in comparison.added_columns.items():
        try:
            type_str = _ibis_type_to_sql(col_type)
            alter_statements.append(f"ALTER TABLE {qualified_name} ADD COLUMN {col_name} {type_str}")
        except Exception as e:
            logger.warning(f"Failed to generate ADD COLUMN statement for {col_name} in {model_name}: {e}")

    # Removed columns: Don't drop (safe mode - preserve columns, NULL for new data)
    # Breaking changes require migrations

    # Apply safe type changes
    for col_name, old_type, new_type in comparison.type_changes:
        if _is_safe_type_cast(old_type, new_type):
            try:
                new_type_str = _ibis_type_to_sql(new_type)
                alter_statements.append(
                    f"ALTER TABLE {qualified_name} ALTER COLUMN {col_name} TYPE {new_type_str} USING CAST({col_name} AS {new_type_str})"
                )
            except Exception as e:
                logger.warning(f"Failed to generate ALTER COLUMN statement for {col_name} in {model_name}: {e}")
        else:
            # Unsafe type change - should have been caught by validation
            logger.warning(
                f"Unsafe type change for {col_name} in {model_name}: {old_type} → {new_type}. " "Migration required."
            )

    if alter_statements:
        batch_sql = "; ".join(alter_statements)
        try:
            _execute_sql_internal(connection, batch_sql)
        except Exception as e:
            # If batch execution fails, try individual statements
            logger.warning(f"Batch schema update failed for {model_name}, trying individual statements: {e}")
            for sql in alter_statements:
                try:
                    _execute_sql_internal(connection, sql)
                except Exception as e2:
                    logger.warning(
                        f"Failed to apply individual schema change for {model_name}: {e2}",
                        exc_info=True,
                    )

    return len(alter_statements)


def _ibis_type_to_sql(ibis_type: any) -> str:
    """
    Convert ibis type to SQL type string.

    Args:
        ibis_type: Ibis type object

    Returns:
        SQL type string
    """
    type_str = str(ibis_type).upper()

    # Map common ibis types to SQL
    type_map = {
        "INT8": "TINYINT",
        "INT16": "SMALLINT",
        "INT32": "INTEGER",
        "INT64": "BIGINT",
        "FLOAT32": "REAL",
        "FLOAT64": "DOUBLE",
        "STRING": "VARCHAR",
        "TIMESTAMP": "TIMESTAMP",
        "DATE": "DATE",
        "BOOLEAN": "BOOLEAN",
        "DECIMAL": "DECIMAL",
        "JSON": "JSON",
    }

    for ibis_name, sql_name in type_map.items():
        if ibis_name in type_str:
            # Handle DECIMAL with precision
            if "DECIMAL" in type_str:
                # Extract precision and scale if present
                if "(" in type_str:
                    return type_str
                return "DECIMAL(18, 2)"  # Default precision
            return sql_name

    # Default to VARCHAR for unknown types
    return "VARCHAR"
