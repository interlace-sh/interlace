"""
Ephemeral materialisation - register in-memory for downstream use.

Phase 0: Basic ephemeral materialisation (register with DuckDB via ibis).
"""

from interlace.materialization.base import Materializer
from interlace.utils.logging import get_logger
from typing import Any
import ibis


class EphemeralMaterializer(Materializer):
    """Ephemeral materializer - registers data in-memory."""

    def __init__(self):
        self.logger = get_logger("interlace.materialization.ephemeral")

    def materialise(
        self, data: ibis.Table, model_name: str, schema: str, connection: Any = None, **kwargs
    ) -> None:
        """
        Materialise data as ephemeral (register with ibis connection using alias).

        Uses `.alias()` to name the table expression, then registers it with the connection
        so downstream models can reference it by name in SQL queries.

        For SQL models: `connection.sql(...).alias(model_name)` then register
        For Python models: `ibis.memtable(data).alias(model_name)` then register

        Args:
            data: Table expression to register (from SQL query or Python data)
            model_name: Model name (used as alias/table name)
            schema: Schema name (not used for ephemeral, but kept for consistency)
            connection: ibis connection (required)
            **kwargs: Additional parameters
        """
        if connection is None:
            raise ValueError("Connection required for ephemeral materialisation")

        # Get fields for schema application (if provided)
        fields = kwargs.get("fields")

        # Try to create table directly from ibis.Table first (avoids DataFrame materialization)
        # This is the most efficient path - no execute() needed
        if not fields:
            try:
                # Try creating temp table directly from ibis expression (more efficient)
                connection.create_table(model_name, obj=data, temp=True)
                return  # Success - table created without DataFrame materialization
            except (TypeError, AttributeError, ValueError, NotImplementedError):
                # Backend doesn't support direct ibis.Table creation - fall back to DataFrame
                pass
            except Exception as e:
                # Other errors - log and fall back to DataFrame approach
                self.logger.debug(
                    f"Could not create ephemeral table directly from ibis expression for '{model_name}': {e}. "
                    f"Falling back to DataFrame materialization."
                )

        # Fallback: Execute ibis expression to materialise data, then create table
        # Note: Some backends require materialised data (DataFrame) for create_table(obj=)
        try:
            df = data.execute()
        except Exception as e:
            error_msg = (
                f"Failed to execute ibis expression for ephemeral materialization "
                f"of model '{model_name}': {str(e)}"
            )
            self.logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from e

        # Apply fields if provided (adds/replaces fields in schema)
        # Apply schema directly to DataFrame to avoid re-execution
        if fields:
            try:
                from interlace.utils.schema_utils import (
                    fields_to_ibis_schema,
                    merge_schemas,
                    apply_schema_to_dataframe
                )

                # Convert fields to ibis schema
                fields_schema = fields_to_ibis_schema(fields)
                if fields_schema:
                    # Get current schema from data
                    current_schema = data.schema()
                    # Merge fields schema into current schema (add/replace)
                    merged_schema = merge_schemas(current_schema, fields_schema)
                    # Apply schema directly to DataFrame (no re-execution)
                    df = apply_schema_to_dataframe(df, merged_schema)
            except Exception as e:
                # Log error with full details - schema mismatches can cause downstream failures
                self.logger.error(
                    f"Failed to apply fields to ephemeral model '{model_name}': {e}. "
                    f"Continuing with original schema, but downstream models may fail.",
                    exc_info=True
                )

        # Create temporary table with the data
        # This makes it available for downstream models to reference by model_name in SQL queries
        # If this fails, raise the exception so the task is marked as failed
        connection.create_table(model_name, obj=df, temp=True)
