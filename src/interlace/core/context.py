"""
Execution context for models.

Provides access to connection and other execution-time context without requiring
models to declare them as parameters.
"""

from contextvars import ContextVar
from typing import Any

import ibis

# Context variable for the current execution connection
_current_connection: ContextVar[ibis.BaseBackend | None] = ContextVar("current_connection", default=None)


def get_connection() -> ibis.BaseBackend:
    """
    Get the current execution connection from context.

    Models can call this function to access the connection without declaring
    it as a parameter.

    Returns:
        ibis.BaseBackend: The current execution connection

    Raises:
        RuntimeError: If no connection is set in the context
    """
    conn = _current_connection.get()
    if conn is None:
        raise RuntimeError(
            "No connection available in execution context. "
            "This function should only be called during model execution."
        )
    return conn


def set_connection(connection: ibis.BaseBackend) -> None:
    """
    Set the connection in the execution context.

    This is called by the executor before running models.

    Args:
        connection: The ibis connection to set in context
    """
    _current_connection.set(connection)


def sql(query: str) -> Any:
    """
    Execute raw SQL query using the current execution connection.

    Simple interface for models to execute raw SQL. Gets the connection
    from execution context automatically.

    Args:
        query: SQL query string to execute

    Returns:
        Query result (backend-dependent)

    Raises:
        RuntimeError: If no connection available in execution context
        ValueError: If connection doesn't support SQL execution
    """
    connection = get_connection()
    return _execute_sql_internal(connection, query)


def _execute_sql_internal(connection: ibis.BaseBackend, query: str) -> Any:
    """
    Internal helper to execute raw SQL on an ibis connection.

    **Purpose**: Execute DDL statements (CREATE, DROP, ALTER, MERGE, INSERT, etc.)
    that require immediate execution and side effects.

    **Uses**: `sql().execute()` for deferred execution that allows composition,
    falling back to `raw_sql()` if `sql()` is not available or doesn't work.

    **Rationale**:
    - `sql()` returns ibis.Table (deferred execution) which is more consistent
    - Allows query composition and optimization before execution
    - Works for both DDL and SELECT statements uniformly
    - Falls back to `raw_sql()` for immediate execution if needed

    **When to use**:
    - DDL statements: CREATE, DROP, ALTER, MERGE, INSERT
    - Statements that must execute immediately for side effects

    **When NOT to use** (use `connection.sql()` directly instead):
    - SELECT queries that return data (use `connection.sql()` directly)
    - Queries that should be composable/chainable (returns ibis.Table)

    For DuckDB: sql().execute() executes immediately and returns None for DDL.
    For other backends: sql().execute() may return results.

    Args:
        connection: ibis connection backend
        query: SQL query string to execute (DDL statements)

    Returns:
        Query result (backend-dependent, usually None for DDL)

    Raises:
        ValueError: If connection doesn't support sql() or raw_sql()

    See also: `connection.sql()` for SELECT queries that return ibis.Table
    """
    # Try sql().execute() first (deferred execution - more flexible)
    # This returns ibis.Table for SELECT, executes immediately for DDL
    if hasattr(connection, "sql"):
        try:
            # sql() returns ibis.Table expression (deferred execution)
            table_expr = connection.sql(query)
            # For DDL statements, execute() will execute immediately
            # For SELECT statements, execute() returns DataFrame/result
            result = table_expr.execute()
            # DDL statements typically return None
            return result
        except (AttributeError, TypeError, Exception):
            # If sql().execute() doesn't work for this query, fall back to raw_sql()
            # This might happen for some backends or specific query types
            pass

    # Fall back to raw_sql() for immediate execution
    if hasattr(connection, "raw_sql"):
        # Standard ibis method for arbitrary SQL (CREATE, DROP, etc.)
        # For DuckDB: raw_sql() executes immediately and returns the connection itself
        # For other backends: raw_sql() may return a cursor that needs execution
        result = connection.raw_sql(query)

        # DuckDB returns the connection itself (execution already happened)
        # Check if result is the same object as connection (DuckDB case)
        if result is connection:
            # DuckDB - already executed, just return None for DDL statements
            return None

        # For other backends, check if it's a cursor that needs execution
        # Only call execute() if result is not the connection itself
        if hasattr(result, "execute") and result is not connection:
            try:
                # Try calling execute() without args (for cursors)
                return result.execute()
            except TypeError:
                # If execute() requires arguments, it's not a simple cursor
                # Return result as-is (execution may have already happened)
                return result

        # Return the result as-is (might be a cursor that auto-executed or connection)
        return result
    else:
        raise ValueError(
            f"Cannot execute SQL on connection type {type(connection)}. "
            "Connection must support sql() or raw_sql() method. "
            "See https://ibis-project.org/how-to/extending/sql"
        )
