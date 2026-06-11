"""Source models — load CSV files into DuckDB."""

from interlace import model, get_connection


@model(name="menu_items", strategy="replace")
def menu_items():
    """Load the coffee shop menu. Replace fully on each run."""
    conn = get_connection()
    return conn.read_csv("data/menu_items.csv")


@model(name="sales", strategy="append")
def sales():
    """Load daily sales transactions. Append new rows each run."""
    conn = get_connection()
    return conn.read_csv("data/sales.csv")
