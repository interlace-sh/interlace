"""
Source models demonstrating all strategies and materialization types.

Covers:
- All strategies: merge_by_key, append, replace, none
- All materialization types: table, view, ephemeral
- Schema evolution: fields parameter
- CSV ingestion
"""

from interlace import model
from pathlib import Path
import pandas as pd
import ibis


def _get_data_path(filename: str) -> str:
    """Get path to data file relative to project root."""
    project_root = Path(__file__).parent.parent
    return str(project_root / "data" / filename)


# Strategy: merge_by_key
@model(
    name="customers",
    schema="public",
    materialise="table",
    strategy="merge_by_key",
    primary_key="customer_id",
    description="Customer data with merge strategy"
)
def customers():
    """Customer data with merge_by_key strategy."""
    # Use DuckDB's CSV reading via ibis for better performance
    from interlace.core.context import get_connection
    connection = get_connection()
    csv_path = _get_data_path("customers.csv")
    return connection.sql(f"SELECT * FROM read_csv_auto('{csv_path}')")


# Strategy: append
@model(
    name="events",
    schema="public",
    materialise="table",
    strategy="append",
    description="Event log with append strategy"
)
def events():
    """Event log with append strategy."""
    # Use DuckDB's CSV reading via ibis for better performance
    from interlace.core.context import get_connection
    connection = get_connection()
    csv_path = _get_data_path("events.csv")
    return connection.sql(f"SELECT * FROM read_csv_auto('{csv_path}')")


# Strategy: replace
@model(
    name="products",
    schema="public",
    materialise="table",
    strategy="replace",
    description="Product catalog with replace strategy"
)
def products():
    """Product catalog with replace strategy."""
    # Use DuckDB's CSV reading via ibis for better performance
    from interlace.core.context import get_connection
    connection = get_connection()
    csv_path = _get_data_path("products.csv")
    return connection.sql(f"SELECT * FROM read_csv_auto('{csv_path}')")


# Strategy: none (no strategy)
@model(
    name="reference_data",
    schema="public",
    materialise="table",
    description="Reference data (no strategy - uses default replace behavior)"
)
def reference_data():
    """Reference data with no strategy (defaults to replace)."""
    # Use DuckDB's CSV reading via ibis for better performance
    from interlace.core.context import get_connection
    connection = get_connection()
    csv_path = _get_data_path("reference_data.csv")
    # Use DuckDB's read_csv function via SQL
    return connection.sql(f"SELECT * FROM read_csv_auto('{csv_path}')")


# View materialization
@model(
    name="active_customers_view",
    schema="public",
    materialise="view",
    description="View of active customers"
)
def active_customers_view(customers: ibis.Table) -> ibis.Table:
    """View of active customers."""
    return customers.filter(customers.status == "active")


# Ephemeral materialization
@model(
    name="temp_calculation",
    schema="public",
    materialise="ephemeral",
    description="Temporary calculation (ephemeral)"
)
def temp_calculation(products: ibis.Table) -> ibis.Table:
    """Temporary calculation (ephemeral)."""
    return products.select([
        products.product_id,
        products.name,
        (products.price * 1.1).name("price_with_tax")
    ])


# Schema evolution: explicit fields
@model(
    name="orders",
    schema="public",
    materialise="table",
    strategy="merge_by_key",
    primary_key="order_id",
    fields={
        "order_id": "int64",
        "customer_id": "int64",
        "product_id": "int64",
        "quantity": "int64",
        "amount": "float64",
        "order_date": "date",
        "status": "string"
    },
    description="Orders with explicit fields schema"
)
def orders():
    """Orders with explicit table schema for schema evolution testing."""
    # Use DuckDB's CSV reading via ibis for better performance
    from interlace.core.context import get_connection
    connection = get_connection()
    csv_path = _get_data_path("orders.csv")
    return connection.sql(f"SELECT * FROM read_csv_auto('{csv_path}')")


# Dynamic merge table with random data and overlapping IDs
@model(
    name="dynamic_merge_demo",
    schema="public",
    materialise="table",
    strategy="merge_by_key",
    primary_key="record_id",
    fields={
        "record_id": "int64",
        "name": "string",
        "value": "float64",
        "status": "string",
        "updated_at": "date",
        "score": "int64"
    },
    description="Dynamic merge demonstration - generates random data with overlapping IDs"
)
def dynamic_merge_demo():
    """
    Generate random data with overlapping IDs to demonstrate merge_by_key strategy.
    
    On each run:
    - Some IDs will overlap (existing records get updated)
    - Some IDs will be new (new records get inserted)
    - This demonstrates the merge strategy in action
    
    The model generates 10-15 records with a mix of:
    - Existing IDs (1-30) - will be updated
    - New IDs (starting from 31+) - will be inserted
    Each run expands the ID range to ensure new IDs are always available.
    """
    import random
    import ibis
    from datetime import datetime, timedelta
    
    # Use current timestamp as seed for randomness
    # This ensures we get different data each run
    current_time = datetime.now()
    random.seed(int(current_time.timestamp()))
    
    # Generate 10-15 records
    num_records = random.randint(10, 15)
    records = []
    
    # Strategy: Mix of existing IDs (1-15) and new IDs (starting from high number)
    # This ensures we always have both updates and inserts
    # Use a small existing pool to ensure overlap with early records
    existing_id_pool = list(range(1, 16))  # IDs 1-15 (likely to exist)
    
    # Determine how many should be existing vs new
    # 30-40% existing (updates), 60-70% new (inserts)
    # This ensures more new records are created
    existing_count = random.randint(
        max(1, int(num_records * 0.3)),  # At least 1, at least 30%
        min(num_records - 3, int(num_records * 0.4))  # At most 40%, ensure at least 3 new
    )
    new_count = num_records - existing_count
    
    # Select existing IDs (will be updated if they exist)
    selected_existing = random.sample(existing_id_pool, existing_count)
    
    # Generate new IDs starting from a high number to ensure they're truly new
    # Start from 100+ to avoid conflicts with existing records
    # This ensures new IDs are always outside the existing range
    next_new_id = 100 + random.randint(0, 10)  # Start between 100-110
    selected_new = []
    for _ in range(new_count):
        # Add some randomness to new ID generation
        # Sometimes skip IDs to create gaps
        if random.random() < 0.3:  # 30% chance to skip
            next_new_id += random.randint(1, 5)
        selected_new.append(next_new_id)
        next_new_id += random.randint(1, 3)  # Increment by 1-3
    
    # Combine and shuffle to mix updates and inserts
    all_ids = selected_existing + selected_new
    random.shuffle(all_ids)
    
    base_date = datetime(2024, 1, 1)
    
    for record_id in all_ids:
        # Generate random data for each record
        # Values will change on each run, even for overlapping IDs
        records.append({
            "record_id": record_id,
            "name": f"Record_{record_id}",
            "value": round(random.uniform(10.0, 100.0), 2),
            "status": random.choice(["active", "inactive", "pending", "archived"]),
            "updated_at": (base_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
            "score": random.randint(0, 100),
        })
    
    # Convert to ibis table
    return ibis.memtable(records)

