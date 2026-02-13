"""
View materialisation - create view in catalog.

Phase 0: Basic view materialisation with DuckDB.
"""

from pathlib import Path
from typing import Any

import ibis

from interlace.core.context import _execute_sql_internal
from interlace.materialization.base import Materializer
from interlace.utils.hashing import has_file_changed
from interlace.utils.logging import get_logger


class ViewMaterializer(Materializer):
    """View materializer - creates view in catalog."""

    def __init__(self):
        self.logger = get_logger("interlace.materialization.view")

    def materialise(self, data: ibis.Table, model_name: str, schema: str, connection: Any, **kwargs) -> None:
        """
        Materialise data as view.

        Phase 0: Create view in DuckDB using ibis.
        Only recreates view if file hash has changed.

        Args:
            data: Table to materialise as view
            model_name: Model name (view name)
            schema: Schema name
            connection: ibis connection (DuckDB backend)
            **kwargs: Additional parameters (may include file_path, file_hash)
        """
        # Check if view should be recreated (file hash changed)
        file_path = kwargs.get("file_path")
        if file_path:
            try:
                # Try to get state connection from kwargs (passed from executor)
                state_conn = kwargs.get("state_connection")
                if state_conn:
                    if not has_file_changed(state_conn, model_name, schema, Path(file_path)):
                        # File hasn't changed, skip view recreation
                        self.logger.debug(f"View {model_name} unchanged, skipping recreation")
                        return
            except Exception as e:
                self.logger.debug(f"Could not check file hash for view {model_name}: {e}, recreating view")
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

        # Apply fields if provided (adds/replaces fields in schema)
        # For views, we execute to DataFrame, apply schema via memtable, then use the new table
        fields = kwargs.get("fields")
        if fields:
            try:
                from interlace.utils.schema_utils import fields_to_ibis_schema, merge_schemas

                # Convert fields to ibis schema
                fields_schema = fields_to_ibis_schema(fields)
                if fields_schema:
                    # Get current schema from data
                    current_schema = data.schema()
                    # Merge fields schema into current schema (add/replace)
                    merged_schema = merge_schemas(current_schema, fields_schema)
                    # Execute to DataFrame, then recreate with merged schema using memtable
                    df = data.execute()
                    # Use ibis.memtable() with schema to recreate table with proper types
                    data = ibis.memtable(df, schema=merged_schema)
            except Exception as e:
                self.logger.warning(f"Failed to apply fields to view: {e}")

        # Create or replace view
        qualified_name = f"{schema}.{model_name}"

        try:
            # Drop view first if it exists
            try:
                if hasattr(connection, "drop_view"):
                    # Try with schema parameter if supported
                    try:
                        connection.drop_view(model_name, schema=schema, force=True)
                    except (TypeError, AttributeError):
                        # Fallback: use qualified name
                        connection.drop_view(qualified_name, force=True)
                else:
                    # Use SQL to drop view (DDL - immediate execution)
                    _execute_sql_internal(connection, f"DROP VIEW IF EXISTS {qualified_name}")
            except Exception:
                # View might not exist, that's fine
                pass

            # Create view - views store SQL queries, so we compile the ibis expression to SQL
            # Dependencies should already be registered/materialised, so compiled SQL references actual tables
            if hasattr(connection, "create_view"):
                try:
                    # Use database parameter for schema (like create_table)
                    connection.create_view(model_name, obj=data, database=schema, overwrite=True)
                    return
                except (TypeError, AttributeError):
                    try:
                        connection.create_view(qualified_name, obj=data, overwrite=True)
                        return
                    except Exception:
                        pass
                except Exception:
                    pass

            sql_expr = str(connection.compile(data))
            _execute_sql_internal(connection, f"CREATE OR REPLACE VIEW {qualified_name} AS {sql_expr}")
        except Exception:
            raise
