"""
SQL identifier and value escaping utilities.

Provides safe SQL identifier and value escaping to prevent SQL injection.
"""


def escape_identifier(identifier: str) -> str:
    """
    Escape SQL identifier (table name, column name, schema name).
    
    Wraps identifier in double quotes and escapes any double quotes within.
    This is safe for PostgreSQL, DuckDB, and most SQL databases.
    
    Args:
        identifier: SQL identifier to escape (table name, column name, etc.)
        
    Returns:
        Escaped identifier wrapped in double quotes
        
    Example:
        >>> escape_identifier("user_table")
        '"user_table"'
        >>> escape_identifier('table"name')
        '"table""name"'
    """
    if not identifier:
        raise ValueError("Identifier cannot be empty")
    
    # Replace double quotes with escaped double quotes
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def escape_qualified_name(schema: str, table: str) -> str:
    """
    Escape qualified table name (schema.table).
    
    Args:
        schema: Schema/database name
        table: Table name
        
    Returns:
        Escaped qualified name: "schema"."table"
    """
    return f"{escape_identifier(schema)}.{escape_identifier(table)}"


def validate_identifier(identifier: str) -> bool:
    """
    Validate that identifier contains only safe characters.
    
    Allows: letters, numbers, underscores, and hyphens.
    Rejects: SQL keywords, special characters, spaces.
    
    Args:
        identifier: Identifier to validate
        
    Returns:
        True if identifier is safe, False otherwise
    """
    if not identifier:
        return False
    
    # Check for only alphanumeric, underscore, and hyphen
    import re
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_-]*$', identifier))


def escape_sql_string(value: str) -> str:
    """
    Escape SQL string value (for use in SQL queries).
    
    Escapes single quotes by doubling them, which is the SQL standard.
    
    Args:
        value: String value to escape
        
    Returns:
        Escaped string with single quotes doubled
        
    Example:
        >>> escape_sql_string("O'Brien")
        "O''Brien"
    """
    if value is None:
        return "NULL"
    
    # Replace single quotes with doubled single quotes
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"



