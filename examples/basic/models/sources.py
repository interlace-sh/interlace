"""
Basic source models - CSV ingestion examples.

Demonstrates:
- CSV file ingestion
- Different strategies (append, replace, merge_by_key)
- Basic materialization (table)
"""

from interlace import model
from pathlib import Path


def _get_data_path(filename: str) -> str:
    """Get path to data file relative to project root."""
    project_root = Path(__file__).parent.parent
    return str(project_root / "data" / filename)


@model(
    name="users",
    schema="public",
    materialise="table",
    strategy="append",
    description="User data from CSV file"
)
def users():
    """Read user data from CSV file using DuckDB's read_csv_auto."""
    from interlace.core.context import get_connection
    connection = get_connection()
    csv_path = _get_data_path("users.csv")
    return connection.sql(f"SELECT * FROM read_csv_auto('{csv_path}')")


@model(
    name="orders",
    schema="public",
    materialise="table",
    strategy="merge_by_key",
    primary_key="order_id",
    description="Order data from CSV file with merge strategy"
)
def orders():
    """Read order data from CSV file with merge strategy."""
    from interlace.core.context import get_connection
    connection = get_connection()
    csv_path = _get_data_path("orders.csv")
    return connection.sql(f"SELECT * FROM read_csv_auto('{csv_path}')")


@model(
    name="products",
    schema="public",
    materialise="table",
    strategy="replace",
    description="Product catalog from CSV (fully replaced on each run)"
)
def products():
    """Read product data from CSV file with replace strategy."""
    from interlace.core.context import get_connection
    connection = get_connection()
    csv_path = _get_data_path("products.csv")
    return connection.sql(f"SELECT * FROM read_csv_auto('{csv_path}')")

