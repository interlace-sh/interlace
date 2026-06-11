#!/usr/bin/env python3
"""
Generate TPC-DS Query SQL Files

This script generates all TPC-DS query SQL files by extracting them from
DuckDB's tpcds_queries() function, and automatically detects and adds schema
definitions for queries with NULL columns or Decimal type issues.

Usage:
    python generate_queries.py

This will create tpcds_q01.sql through tpcds_q99.sql in models/tpcds/
"""

import duckdb
from pathlib import Path
import json
import re


def infer_schema_from_query(conn, query_sql: str, query_nr: int) -> dict:
    """
    Execute query and infer schema, detecting NULL columns and Decimal types.
    
    Args:
        conn: DuckDB connection with TPC-DS data loaded
        query_sql: SQL query text
        query_nr: Query number (for error messages)
        
    Returns:
        dict: Schema mapping column names to types (e.g., {"col1": "str", "col2": "int"})
    """
    schema = {}
    
    try:
        # Execute query to get result
        result = conn.execute(query_sql).fetchdf()
        
        if result.empty:
            # Empty result - try to get schema from describe
            try:
                # Wrap query in CTE and describe
                describe_query = f"DESCRIBE SELECT * FROM ({query_sql}) AS q"
                desc_result = conn.execute(describe_query).fetchdf()
                for _, row in desc_result.iterrows():
                    col_name = row['column_name']
                    col_type = row['column_type']
                    # Convert DuckDB types to schema types
                    schema[col_name] = _duckdb_type_to_schema_type(col_type)
            except Exception:
                # If describe fails, return empty schema
                pass
        else:
            # Get column info from result
            for col_name in result.columns:
                col_dtype = str(result[col_name].dtype)
                
                # Check if column is all NULL
                if result[col_name].isna().all():
                    # Infer type from column name or use string as default
                    inferred_type = _infer_type_from_name(col_name)
                    schema[col_name] = inferred_type
                else:
                    # Convert pandas dtype to schema type
                    schema[col_name] = _pandas_dtype_to_schema_type(col_dtype)
    
    except Exception as e:
        # If query execution fails, try to parse SELECT clause for column names
        # This is a fallback for queries that might fail without data
        print(f"Warning: Could not execute query {query_nr} to infer schema: {e}")
        schema = _infer_schema_from_select(query_sql)
    
    return schema


def _duckdb_type_to_schema_type(duckdb_type: str) -> str:
    """Convert DuckDB type string to schema type."""
    duckdb_type = duckdb_type.upper()
    
    if "VARCHAR" in duckdb_type or "TEXT" in duckdb_type or "CHAR" in duckdb_type:
        return "str"
    elif "INTEGER" in duckdb_type or "INT" in duckdb_type:
        return "int"
    elif "BIGINT" in duckdb_type:
        return "int"
    elif "DOUBLE" in duckdb_type or "FLOAT" in duckdb_type or "REAL" in duckdb_type:
        return "float"
    elif "DECIMAL" in duckdb_type or "NUMERIC" in duckdb_type:
        return "float"  # Use float instead of decimal to avoid conversion issues
    elif "BOOLEAN" in duckdb_type or "BOOL" in duckdb_type:
        return "bool"
    elif "DATE" in duckdb_type:
        return "date"
    elif "TIMESTAMP" in duckdb_type:
        return "datetime"
    else:
        return "str"  # Default to string


def _pandas_dtype_to_schema_type(pandas_dtype: str) -> str:
    """Convert pandas dtype to schema type."""
    pandas_dtype = str(pandas_dtype).lower()
    
    if "int" in pandas_dtype:
        return "int"
    elif "float" in pandas_dtype:
        return "float"
    elif "bool" in pandas_dtype:
        return "bool"
    elif "datetime" in pandas_dtype or "date" in pandas_dtype:
        return "datetime"
    elif "object" in pandas_dtype or "string" in pandas_dtype:
        return "str"
    else:
        return "str"  # Default to string


def _infer_type_from_name(col_name: str) -> str:
    """Infer type from column name when all values are NULL."""
    col_lower = col_name.lower()
    
    # Common patterns
    if any(x in col_lower for x in ["_id", "_sk", "_key", "count", "cnt"]):
        return "int"
    elif any(x in col_lower for x in ["_name", "_desc", "_type", "state", "country", "county"]):
        return "str"
    elif any(x in col_lower for x in ["_price", "_cost", "_amount", "_sales", "sum", "avg", "min", "max"]):
        return "float"
    elif any(x in col_lower for x in ["_date", "_time"]):
        return "datetime"
    else:
        return "str"  # Default to string


def _infer_schema_from_select(query_sql: str) -> dict:
    """Fallback: try to infer schema from SELECT clause."""
    schema = {}
    
    # Extract SELECT clause
    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query_sql, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return schema
    
    select_clause = select_match.group(1)
    
    # Split by comma, but be careful with nested functions
    # Simple approach: split by comma not inside parentheses
    parts = []
    current = ""
    depth = 0
    for char in select_clause:
        if char == '(':
            depth += 1
            current += char
        elif char == ')':
            depth -= 1
            current += char
        elif char == ',' and depth == 0:
            parts.append(current.strip())
            current = ""
        else:
            current += char
    if current.strip():
        parts.append(current.strip())
    
    # Extract column names (handle aliases)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        
        # Extract alias (AS alias or space-separated)
        alias_match = re.search(r'\s+AS\s+(\w+)', part, re.IGNORECASE)
        if alias_match:
            col_name = alias_match.group(1)
        else:
            # Try to get last word as alias
            words = part.split()
            if len(words) > 1:
                col_name = words[-1]
            else:
                # Use the whole expression, cleaned up
                col_name = re.sub(r'[^a-zA-Z0-9_]', '', part.split('.')[-1] if '.' in part else part)
        
        if col_name:
            # Infer type from expression
            col_lower = part.lower()
            if any(x in col_lower for x in ["count(", "sum(", "min(", "max(", "avg("]):
                if "count" in col_lower:
                    schema[col_name] = "int"
                else:
                    schema[col_name] = "float"
            elif any(x in col_lower for x in ["_id", "_sk", "_key"]):
                schema[col_name] = "int"
            elif any(x in col_lower for x in ["_name", "_desc", "state", "country"]):
                schema[col_name] = "str"
            else:
                schema[col_name] = "str"  # Default
    
    return schema


def format_query_header(query_nr: int, query_sql: str, fields: dict = None) -> str:
    """
    Format the query SQL with proper header metadata.
    
    Args:
        query_nr: Query number (1-99)
        query_sql: The SQL query text
        fields: Optional fields dict to add as annotation
        
    Returns:
        str: Formatted SQL with header
    """
    # Extract query name/description from first comment if present
    query_name = f"TPC-DS Query {query_nr:02d}"
    
    # Try to extract description from query comments
    first_line = query_sql.strip().split('\n')[0] if query_sql.strip() else ""
    if first_line.startswith('--'):
        # Use the comment as description
        query_name = first_line[2:].strip()
    
    header = f"""-- {query_name}
-- @name: tpcds_q{query_nr:02d}
-- @schema: tpcds
-- @materialize: table
-- @strategy: replace
-- @dependencies: tpcds_data
"""
    
    # Add fields annotation if provided
    if fields:
        # Format as JSON for readability
        schema_json = json.dumps(fields, indent=2)
        # Convert to single line with proper escaping for SQL comment
        schema_line = json.dumps(fields)
        header += f"-- @fields: {schema_line}\n"
    
    header += "\n"
    
    return header + query_sql.strip() + "\n"


def generate_queries():
    """
    Generate all TPC-DS query SQL files with schema detection.
    """
    # Get project root directory
    project_root = Path(__file__).parent
    queries_dir = project_root / "models" / "tpcds"
    
    # Ensure directory exists
    queries_dir.mkdir(parents=True, exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect()
    
    try:
        # Load TPC-DS data (scale factor 1)
        print("Loading TPC-DS data (this may take a moment)...")
        try:
            conn.execute("CALL dsdgen(sf=1)")
            print("TPC-DS data loaded successfully.")
        except Exception as e:
            print(f"Warning: Could not load TPC-DS data: {e}")
            print("Schema detection will be limited without data.")
        
        # Get all queries from tpcds_queries() function
        result = conn.execute("SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr").fetchall()
        
        if not result:
            print("Warning: No queries found. Make sure DuckDB has TPC-DS support.")
            return
        
        print(f"Found {len(result)} queries. Generating SQL files with schema detection...")
        
        generated_count = 0
        schemas_added = 0
        
        for query_nr, query_sql in result:
            # Infer schema for this query
            fields = infer_schema_from_query(conn, query_sql, query_nr)
            
            # Only add schema if we detected columns (to avoid empty schemas)
            schema_to_add = fields if fields else None
            if schema_to_add:
                schemas_added += 1
            
            # Format query with header and schema
            formatted_query = format_query_header(query_nr, query_sql, fields=schema_to_add)
            
            # Write to file
            query_file = queries_dir / f"tpcds_q{query_nr:02d}.sql"
            query_file.write_text(formatted_query, encoding='utf-8')
            
            generated_count += 1
            if generated_count % 10 == 0:
                print(f"Generated {generated_count} queries... ({schemas_added} with schemas)")
        
        print(f"\nSuccessfully generated {generated_count} query files in {queries_dir}")
        print(f"Files: tpcds_q01.sql through tpcds_q{query_nr:02d}.sql")
        print(f"Added schema definitions to {schemas_added} queries")
        
    except Exception as e:
        print(f"Error generating queries: {e}")
        print("\nMake sure DuckDB has TPC-DS support enabled.")
        print("You may need to install DuckDB with TPC-DS extension or use a different method.")
        raise
    
    finally:
        conn.close()


if __name__ == "__main__":
    generate_queries()
