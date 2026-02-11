"""
TPC-H Data Generation Model.

Generates TPC-H benchmark data using DuckDB's built-in dbgen function.
This creates the standard TPC-H tables: nation, region, supplier, customer,
part, partsupp, orders, lineitem.
"""

from interlace import model, sql
from interlace.core.context import get_connection
from interlace.utils.logging import get_logger

logger = get_logger(__name__)

@model(
    name="tpch_data",
    schema="main",
    materialise="none",  # Tables created directly by dbgen, no materialisation needed
    description="Generate TPC-H benchmark data using DuckDB dbgen - creates tables directly",
    tags=["tpch", "benchmark", "data-generation"]
)
def tpch_data():
    """
    Generate TPC-H benchmark data.
    
    Uses DuckDB's built-in dbgen function to create TPC-H tables.
    Scale factor (sf) controls data size:
    - sf=0.1 ≈ 100MB
    - sf=1 ≈ 1GB
    - sf=10 ≈ 10GB
    - sf=100 ≈ 100GB
    
    Tables created:
    - nation, region (dimensions)
    - supplier, customer, part, partsupp (medium)
    - orders, lineitem (large, main fact tables)
    
    Returns:
        None: Tables are created directly in the database
    """
    # Get scale factor from model config or use default
    # Can be set via @model(sf=1) or via config
    sf = 0.1  # Use smaller scale factor for testing (0.1 ≈ 100MB)
    
    connection = get_connection()

    # Check if core TPC-H tables exist
    tpch_tables = ["nation", "region", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
    result_df = connection.sql(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main';"
    ).execute()
    existing_tables = set(result_df['table_name'].tolist() if 'table_name' in result_df.columns else [])

    if all(tbl in existing_tables for tbl in tpch_tables):
        logger.info("TPC-H tables already exist, skipping dbgen.")
        return None
    
    # Execute dbgen to create TPC-H tables
    sql(f"CALL dbgen(sf={sf})")
    
    # Tables are created directly by dbgen - no return needed
    # Downstream models can reference them directly by name, however listing tpch_data as a dependency is recommended to ensure the tables are created before downstream models run.
    return None


