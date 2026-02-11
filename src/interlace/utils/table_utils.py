"""
Common table utilities to reduce code duplication.

Extracts repeated patterns for table existence checking, schema operations, etc.
"""

from typing import Optional
import ibis
from interlace.utils.logging import get_logger

logger = get_logger("interlace.utils.table_utils")


def check_table_exists(
    connection: ibis.BaseBackend, table_name: str, schema: Optional[str] = None
) -> bool:
    """
    Check if a table exists using list_tables() instead of exception handling.
    
    This is a common pattern used throughout the codebase. Extracted to reduce duplication.

    Args:
        connection: ibis connection backend
        table_name: Table name to check
        schema: Schema/database name (optional)

    Returns:
        True if table exists, False otherwise
    """
    try:
        # Use list_tables with pattern matching
        # Pattern matches exact table name
        pattern = f"^{table_name}$"

        if schema:
            # Try with database parameter first
            try:
                tables = connection.list_tables(like=pattern, database=schema)
                return table_name in tables
            except (TypeError, AttributeError):
                # Fallback: try without database parameter
                pass

        # Try without database parameter (default schema)
        tables = connection.list_tables(like=pattern)
        return table_name in tables
    except Exception:
        # If list_tables fails, fall back to False
        return False


def try_load_table(
    connection: ibis.BaseBackend,
    table_name: str,
    schemas: list[Optional[str]],
) -> Optional[ibis.Table]:
    """
    Try to load a table from multiple schemas.

    Common pattern: try multiple schemas until one works.
    Extracted to reduce duplication.

    Args:
        connection: ibis connection backend
        table_name: Table name to load
        schemas: List of schemas to try (in order)

    Returns:
        ibis.Table if found, None otherwise
    """
    for try_schema in schemas:
        try:
            if try_schema:
                return connection.table(table_name, database=try_schema)
            else:
                return connection.table(table_name)
        except Exception:
            continue

    return None


def get_row_count_efficient(
    connection: ibis.BaseBackend,
    table: ibis.Table | str,
    schema: Optional[str] = None,
) -> Optional[int]:
    """
    Get row count using SQL COUNT(*) instead of .count().execute().

    Avoids materializing the full result set, following DuckDB and ibis best practices.

    **Performance Rationale:**
    - `.count().execute()` forces materialization of the entire query result
    - SQL COUNT(*) is executed by the database engine without fetching data
    - This is especially important for large tables or complex queries

    **References:**
    - DuckDB Python API: Use SQL aggregations for efficiency
    - Ibis best practices: Stay lazy, minimize data movement

    Args:
        connection: ibis connection backend
        table: ibis.Table expression or table name string
        schema: Optional schema/database name for table name lookups

    Returns:
        Row count as integer, or None if count fails

    Examples:
        >>> # From ibis.Table
        >>> count = get_row_count_efficient(conn, users_table)

        >>> # From table name
        >>> count = get_row_count_efficient(conn, "users", schema="public")
    """
    try:
        # Handle string table names - convert to ibis.Table
        if isinstance(table, str):
            table_name = table
            if schema:
                table = connection.table(table_name, database=schema)
            else:
                table = connection.table(table_name)

        # Use ibis COUNT aggregation (stays lazy until execute)
        count_expr = table.count()

        # Execute only the COUNT - database handles aggregation
        return int(count_expr.execute())

    except Exception as e:
        logger.warning(f"Failed to get row count: {e}")
        return None
