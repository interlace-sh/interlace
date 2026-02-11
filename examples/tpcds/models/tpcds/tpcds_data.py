"""
TPC-DS Data Generation Model.

Generates TPC-DS benchmark data. TPC-DS has 24 tables representing a decision
support system for a retail company.

Note: DuckDB may not have built-in TPC-DS dbgen support. This model provides
a framework that can be adapted to use external TPC-DS tools or manual data loading.
"""

from interlace import model, sql
from interlace.core.context import get_connection
from interlace.utils.logging import get_logger

logger = get_logger(__name__)

@model(
    name="tpcds_data",
    schema="main",
    materialise="none",  # Tables created directly, no materialisation needed
    description="Generate TPC-DS benchmark data - creates 24 tables directly",
    tags=["tpcds", "benchmark", "data-generation"]
)
def tpcds_data():
    """
    Generate TPC-DS benchmark data.
    
    TPC-DS has 24 tables:
    - Dimension tables: date_dim, time_dim, customer, customer_address, 
      customer_demographics, household_demographics, item, promotion, 
      store, store_returns, warehouse, ship_mode, reason, income_band, 
      call_center, web_page, web_site
    - Fact tables: store_sales, store_returns, catalog_sales, 
      catalog_returns, web_sales, web_returns, inventory
    
    Scale factor (sf) controls data size:
    - sf=1 ≈ 1GB
    - sf=10 ≈ 10GB
    - sf=100 ≈ 100GB
    
    Note: If DuckDB doesn't support TPC-DS dbgen, you may need to:
    1. Use external TPC-DS tools to generate data files
    2. Load the data files into DuckDB using COPY or INSERT statements
    3. Or use a different database system that supports TPC-DS
    
    Returns:
        None: Tables are created directly in the database
    """
    sf = 1  # Use smaller scale factor for testing
    
    connection = get_connection()

    # Check if core TPC-DS tables exist
    tpcds_tables = [
        "date_dim", "time_dim", "customer", "customer_address",
        "customer_demographics", "household_demographics", "item", "promotion",
        "store", "warehouse", "ship_mode", "reason", "income_band",
        "call_center", "web_page", "web_site",
        "store_sales", "store_returns", "catalog_sales", "catalog_returns",
        "web_sales", "web_returns", "inventory"
    ]
    
    result_df = connection.sql(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main';"
    ).execute()
    existing_tables = set(result_df['table_name'].tolist() if 'table_name' in result_df.columns else [])

    if all(tbl in existing_tables for tbl in tpcds_tables):
        logger.info("TPC-DS tables already exist, skipping data generation.")
        return None
    
    # Try DuckDB's dbgen for TPC-DS (if supported)
    # If not supported, this will need to be replaced with external tooling
    try:
        sql(f"CALL dsdgen(sf={sf})")
        logger.info("TPC-DS data generated using DuckDB dbgen.")
    except Exception as e:
        logger.warning(
            f"DuckDB dbgen for TPC-DS may not be supported: {e}. "
            "You may need to use external TPC-DS tools to generate data."
        )
        # Alternative: Load from external files
        # sql("COPY date_dim FROM 'data/tpcds/date_dim.csv' (HEADER, DELIMITER '|')")
        # ... load other tables
    
    return None


