"""
Table materialisation - persist to table in catalog.

Phase 0: Basic table materialisation with DuckDB.
"""

import re
from interlace.materialization.base import Materializer
from interlace.core.context import _execute_sql_internal
from interlace.utils.logging import get_logger
from typing import Any
import ibis


class TableMaterializer(Materializer):
    """Table materializer - persists data to table in catalog."""

    def __init__(self):
        self.logger = get_logger("interlace.materialization.table")

    def materialise(
        self, data: ibis.Table, model_name: str, schema: str, connection: Any, **kwargs
    ) -> None:
        """
        Materialise data to table.

        Strategy is applied BEFORE materialisation (in executor), so this receives:
        - If strategy specified: Final merged result (strategy merged new + existing data)
        - If no strategy: Just the query result (replace behavior)

        Materialisation approach:
        - If table doesn't exist: CREATE TABLE
        - If table exists: CREATE OR REPLACE TABLE (writes final result)

        Limitation: CREATE OR REPLACE drops/recreates the entire table even when strategies
        are applied (which already merged data in memory). This is inefficient but necessary
        because strategies return ibis.Table expressions, not SQL. Future optimization:
        strategies could generate MERGE/INSERT/UPDATE SQL directly to avoid full recreation.

        Multi-connection support: If using DuckDB ATTACH (e.g., ATTACH postgres... AS pg_db),
        models can reference attached databases via qualified names (e.g., "SELECT * FROM pg_db.public.orders").
        DuckDB handles the connection routing automatically. See DuckDBConnection._attach_external().

        Args:
            data: Table to materialise (already merged by strategy if applicable)
            model_name: Model name (table name)
            schema: Schema/database name (ibis uses "database" terminology)
            connection: ibis connection backend
            **kwargs: Additional parameters (may include strategy_name)
        """
        # Create database (schema) if it doesn't exist using ibis create_database()
        # ibis uses "database" terminology (maps to "schema" in PostgreSQL)
        try:
            if hasattr(connection, "create_database"):
                connection.create_database(schema, force=False)
            else:
                # Fallback to SQL if create_database not available
                _execute_sql_internal(connection, f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception as e:
            if "already exists" not in str(e).lower() and "duplicate" not in str(e).lower():
                self.logger.warning(f"Database creation issue (may already exist): {e}")

        # Note: Use table name only with database parameter, NOT qualified name + database
        # Using both causes double-qualification (e.g., table named "public.users" in "public" schema)
        # which breaks table access via conn.table('users', database='public')

        # Try to create table directly from ibis.Table first (avoids DataFrame materialization)
        # Fall back to DataFrame materialization if backend doesn't support direct creation
        try:
            # Try creating table directly from ibis expression (more efficient)
            # This avoids materializing to DataFrame when not necessary
            connection.create_table(model_name, obj=data, database=schema, overwrite=True)
            return  # Success - table created without DataFrame materialization
        except (TypeError, AttributeError, ValueError, NotImplementedError):
            # Backend doesn't support direct ibis.Table creation - fall back to DataFrame
            pass
        except Exception as e:
            # Other errors - log and fall back to DataFrame approach
            self.logger.debug(
                f"Could not create table directly from ibis expression for '{model_name}': {e}. "
                f"Falling back to DataFrame materialization."
            )
        
        # Fallback: Execute ibis expression to materialise data, then create table
        # Note: Some backends require materialised data (DataFrame) for create_table(obj=)
        try:
            df = data.execute()
        except Exception as e:
            error_msg = (
                f"Failed to execute ibis expression for materialization "
                f"of model '{model_name}' in schema '{schema}': {str(e)}"
            )
            self.logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from e

        # Apply fields if provided (adds/replaces fields in schema)
        # Apply schema directly to DataFrame to avoid re-execution
        fields = kwargs.get("fields")
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
                self.logger.warning(f"Failed to apply fields: {e}")

        # Check if table exists using list_tables() instead of exception handling
        table_exists = False
        try:
            if hasattr(connection, "list_tables"):
                # Escape special regex chars to match exact table name
                pattern = f"^{re.escape(model_name)}$"
                # Try with database parameter first
                try:
                    tables = connection.list_tables(like=pattern, database=schema)
                    table_exists = model_name in tables
                except (TypeError, AttributeError):
                    # Fallback: try without database parameter
                    tables = connection.list_tables(like=pattern)
                    table_exists = model_name in tables
        except Exception:
            # If list_tables fails, assume table doesn't exist
            pass

        # Use ibis create_table for standard cross-backend support
        # Signature: create_table(name, obj=None, schema=None, database=None, temp=False, overwrite=False)
        # Note: schema= is for table schema (columns/types), database= is for database/schema namespace
        if hasattr(connection, "create_table"):
            # Build qualified name for fallback (schema.table_name)
            qualified_name = f"{schema}.{model_name}" if schema else model_name
            try:
                # Use database parameter for PostgreSQL-style schema namespace
                connection.create_table(
                    model_name,
                    obj=df,
                    database=schema,  # database= is the schema namespace (PostgreSQL-style)
                    overwrite=table_exists,  # CREATE OR REPLACE if exists, CREATE if not
                )
            except (TypeError, AttributeError):
                # Fallback: use qualified name (some backends may not support database parameter)
                connection.create_table(qualified_name, obj=df, overwrite=table_exists)
        else:
            # Fallback: use raw SQL
            # Register DataFrame temporarily for SQL execution
            # Note: This is DuckDB-specific, should use ibis.create_table for cross-backend
            if hasattr(connection, "raw_sql"):
                # For ibis backends without direct DataFrame support, we need a different approach
                # This is a fallback - prefer create_table() above
                raise ValueError(
                    f"Cannot create table: connection {type(connection)} doesn't support create_table(). "
                    "Use ibis backend that supports create_table() with DataFrame objects."
                )
            else:
                raise ValueError("Cannot create table: no suitable method available")
