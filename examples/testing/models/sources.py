"""Source models — load CSV files into DuckDB."""

from interlace import model, get_connection


@model(name="orders", strategy="replace")
def orders():
    """Load order transactions from CSV. Replace fully on each run."""
    conn = get_connection()
    return conn.read_csv("data/orders.csv")


@model(name="inventory", strategy="replace")
def inventory():
    """Load product inventory from CSV. Replace fully on each run."""
    conn = get_connection()
    return conn.read_csv("data/inventory.csv")
