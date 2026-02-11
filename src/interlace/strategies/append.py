"""
Append strategy - always append new data.

Phase 0: SQL-based append using connection.insert() with overwrite=False.
"""

from interlace.strategies.base import Strategy
from interlace.utils.sql_escape import escape_identifier, escape_qualified_name
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ibis


class AppendStrategy(Strategy):
    """Append strategy - always appends new rows using connection.insert() or INSERT SQL."""

    def generate_sql(
        self,
        connection: "ibis.BaseBackend",
        target_table: str,
        schema: str,
        source_table: str,
        **kwargs,
    ) -> str:
        """
        Generate INSERT SQL for append strategy.

        Args:
            connection: ibis connection backend
            target_table: Target table name
            schema: Schema/database name
            source_table: Source table name (temp table with new data)
            **kwargs: Additional parameters
        """
        # Get column names from source table
        source = connection.table(source_table)
        source_schema = source.schema()  # Don't shadow the schema parameter
        # ibis schema() returns Schema object (dict-like) - iterate directly to get column names
        columns = list(source_schema.keys())

        # Build INSERT statement with escaped identifiers to prevent SQL injection
        escaped_target = escape_qualified_name(schema, target_table)
        escaped_source = escape_identifier(source_table)
        insert_cols = ", ".join([escape_identifier(col) for col in columns])
        insert_vals = ", ".join([f"source.{escape_identifier(col)}" for col in columns])

        return f"""
        INSERT INTO {escaped_target} ({insert_cols})
        SELECT {insert_vals} FROM {escaped_source} AS source
        """

    def needs_temp_table(self) -> bool:
        """Append strategy needs temp table for SQL generation."""
        return True
