"""
Merge by key strategy - merge based on primary key.

Phase 0: SQL-based merge using MERGE SQL statement.
"""

from interlace.strategies.base import Strategy
from typing import List, Optional, TYPE_CHECKING
import ibis  # Used for type hints and schema operations
from interlace.utils.sql_escape import escape_identifier, escape_qualified_name

if TYPE_CHECKING:
    from typing import Any


class MergeByKeyStrategy(Strategy):
    """Merge by key strategy - updates existing rows, inserts new ones using MERGE SQL."""

    def generate_sql(
        self,
        connection: ibis.BaseBackend,
        target_table: str,
        schema: str,
        source_table: str,
        primary_key: str | List[str],
        delete_mode: str = "preserve",
        **kwargs,
    ) -> str:
        """
        Generate MERGE SQL statement for merge_by_key strategy.

        MERGE INTO target_table
        USING source_table
        ON (target_table.field = source_table.field)
        WHEN MATCHED THEN UPDATE | DELETE
        WHEN NOT MATCHED THEN INSERT

        Args:
            connection: ibis connection backend
            target_table: Target table name
            schema: Schema/database name
            source_table: Source table name (temp table with new data)
            primary_key: Primary key column(s)
            delete_mode: How to handle deletes ("preserve", "soft", "hard", "delete_missing")
            **kwargs: Additional parameters
        """
        if not primary_key:
            raise ValueError("primary_key required for merge_by_key strategy")

        if isinstance(primary_key, str):
            primary_key = [primary_key]

        # Get column names from source table
        try:
            source = connection.table(source_table)
            source_schema = source.schema()  # Don't shadow the schema parameter
            # ibis schema() returns Schema object (dict-like) - iterate directly to get column names
            columns = list(source_schema.keys())
        except Exception as e:
            # If we can't get schema, we'll use a generic approach
            # This is a fallback - ideally we should always have schema
            raise ValueError(f"Cannot get schema for source table {source_table}: {e}")

        # Validate primary key columns exist in source table
        missing_keys = [pk for pk in primary_key if pk not in columns]
        if missing_keys:
            raise ValueError(
                f"Primary key column(s) {missing_keys} not found in source table '{source_table}'. "
                f"Available columns: {columns}"
            )

        # Build ON condition (merge condition) with proper escaping
        on_conditions = " AND ".join([
            f"target.{escape_identifier(pk)} = source.{escape_identifier(pk)}" 
            for pk in primary_key
        ])

        # Build UPDATE SET clause (update all non-key columns) with proper escaping
        non_key_columns = [col for col in columns if col not in primary_key]
        if non_key_columns:
            update_set = ", ".join([
                f"{escape_identifier(col)} = source.{escape_identifier(col)}" 
                for col in non_key_columns
            ])
        else:
            update_set = None  # No non-key columns to update

        # Build INSERT clause with proper escaping
        insert_cols = ", ".join([escape_identifier(col) for col in columns])
        insert_vals = ", ".join([f"source.{escape_identifier(col)}" for col in columns])

        # Handle delete_mode
        if delete_mode == "delete_missing":
            # Delete rows in target that don't exist in source
            delete_clause = """
        WHEN NOT MATCHED BY SOURCE THEN DELETE"""
        elif delete_mode == "hard":
            # Same as delete_missing for MERGE
            delete_clause = """
        WHEN NOT MATCHED BY SOURCE THEN DELETE"""
        else:
            # preserve, soft - don't delete
            delete_clause = ""

        # Generate MERGE SQL (DuckDB syntax) with proper identifier escaping
        # DuckDB supports MERGE with USING and ON
        escaped_target = escape_qualified_name(schema, target_table)
        escaped_source = escape_identifier(source_table)

        # Only include WHEN MATCHED clause if there are non-key columns to update.
        # When all columns are primary keys, there is nothing to update on match,
        # and 'UPDATE SET *' is not portable across backends.
        if update_set is not None:
            matched_clause = f"\n    WHEN MATCHED THEN UPDATE SET {update_set}"
        else:
            matched_clause = ""
        
        merge_sql = f"""MERGE INTO {escaped_target} AS target
    USING {escaped_source} AS source
    ON ({on_conditions}){matched_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals}){delete_clause}"""

        return merge_sql

    def needs_temp_table(self) -> bool:
        """Merge_by_key strategy needs temp table for SQL generation."""
        return True
