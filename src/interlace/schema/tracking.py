"""
Schema version tracking.

Tracks schema history for models (informative only, no rollback).
"""

from typing import Any

import ibis

from interlace.core.context import _execute_sql_internal
from interlace.utils.logging import get_logger

logger = get_logger("interlace.schema.tracking")


def get_current_schema_version(connection: ibis.BaseBackend, model_name: str, schema_name: str) -> int:
    """
    Get current schema version for a model.

    Args:
        connection: Database connection
        model_name: Model name
        schema_name: Schema name

    Returns:
        Current schema version (0 if no history)
    """
    try:
        from interlace.core.state import _escape_sql_string

        safe_model_name = _escape_sql_string(model_name)
        safe_schema_name = _escape_sql_string(schema_name)
        query = f"""
            SELECT MAX(version) as max_version
            FROM interlace.schema_history
            WHERE model_name = '{safe_model_name}' AND schema_name = '{safe_schema_name}'
        """
        result = connection.sql(query).execute()
        if result is not None and len(result) > 0:
            max_version = result.iloc[0]["max_version"] if hasattr(result, "iloc") else None
            return int(max_version) if max_version is not None else 0
        return 0
    except Exception as e:
        # Graceful degradation: if table doesn't exist, return 0
        error_str = str(e).lower()
        if "does not exist" in error_str or "not found" in error_str or "no such table" in error_str:
            logger.debug(f"Schema history table not found: {e}")
        else:
            logger.debug(f"Could not get schema version for {model_name}: {e}")
        return 0


def track_schema_version(
    connection: ibis.BaseBackend,
    model_name: str,
    schema_name: str,
    schema: ibis.Schema,
    primary_key: str | list[str] | None = None,
) -> int:
    """
    Track schema version in history (informative only, no rollback).

    Args:
        connection: Database connection
        model_name: Model name
        schema_name: Schema name
        schema: Current schema
        primary_key: Optional primary key column(s)

    Returns:
        New schema version number
    """
    try:
        from interlace.core.state import _escape_sql_string

        current_version = get_current_schema_version(connection, model_name, schema_name)
        new_version = current_version + 1

        primary_key_set = set()
        if primary_key:
            if isinstance(primary_key, str):
                primary_key_set.add(primary_key)
            else:
                primary_key_set.update(primary_key)

        safe_model_name = _escape_sql_string(model_name)
        safe_schema_name = _escape_sql_string(schema_name)

        # Insert schema columns into history
        for col_name, col_type in schema.items():
            is_pk = col_name in primary_key_set
            type_str = str(col_type)
            safe_col_name = _escape_sql_string(col_name)
            safe_type_str = _escape_sql_string(type_str)

            insert_query = f"""
                INSERT INTO interlace.schema_history
                (model_name, schema_name, version, column_name, column_type, is_nullable, is_primary_key, detected_at)
                VALUES (
                    '{safe_model_name}',
                    '{safe_schema_name}',
                    {new_version},
                    '{safe_col_name}',
                    '{safe_type_str}',
                    TRUE,
                    {1 if is_pk else 0},
                    CURRENT_TIMESTAMP
                )
            """
            _execute_sql_internal(connection, insert_query)

        return new_version
    except Exception as e:
        # Graceful degradation: if table doesn't exist, return 0
        error_str = str(e).lower()
        if "does not exist" in error_str or "not found" in error_str or "no such table" in error_str:
            logger.debug(f"Schema history table not found: {e}")
        else:
            logger.warning(f"Could not track schema version for {model_name}: {e}")
        return 0


def get_schema_history(
    connection: ibis.BaseBackend,
    model_name: str,
    schema_name: str,
    version: int | None = None,
) -> list[dict[str, Any]]:
    """
    Get schema history for a model.

    Args:
        connection: Database connection
        model_name: Model name
        schema_name: Schema name
        version: Optional version number (if None, returns all versions)

    Returns:
        List of schema history records
    """
    try:
        from interlace.core.state import _escape_sql_string

        safe_model_name = _escape_sql_string(model_name)
        safe_schema_name = _escape_sql_string(schema_name)
        if version is not None:
            query = f"""
                SELECT * FROM interlace.schema_history
                WHERE model_name = '{safe_model_name}'
                AND schema_name = '{safe_schema_name}'
                AND version = {version}
                ORDER BY column_name
            """
        else:
            query = f"""
                SELECT * FROM interlace.schema_history
                WHERE model_name = '{safe_model_name}'
                AND schema_name = '{safe_schema_name}'
                ORDER BY version DESC, column_name
            """
        result = connection.sql(query).execute()
        if result is not None and len(result) > 0:
            return result.to_dict("records") if hasattr(result, "to_dict") else []
        return []
    except Exception as e:
        # Graceful degradation: if table doesn't exist, return empty list
        error_str = str(e).lower()
        if "does not exist" in error_str or "not found" in error_str or "no such table" in error_str:
            logger.debug(f"Schema history table not found: {e}")
        else:
            logger.debug(f"Could not get schema history for {model_name}: {e}")
        return []
