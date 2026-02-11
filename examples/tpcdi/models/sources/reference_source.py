"""
TPC-DI Source Models: Reference/Dimension Data Extraction

Extracts date and time dimension data from source CSV files.
These are typically pre-generated reference tables.
"""

from interlace import model
from pathlib import Path
import pandas as pd


def _get_data_path(filename: str) -> str:
    """Get path to source data file."""
    project_root = Path(__file__).parent.parent.parent
    return str(project_root / "data" / "raw" / filename)


@model(
    name="src_dim_date",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract date dimension reference data",
    tags=["source", "extract", "dimension", "date"]
)
def src_dim_date():
    """
    Extract date dimension data from source CSV.

    The date dimension contains one row per calendar day
    with various date attributes for analysis.

    Returns:
        pandas.DataFrame: Date dimension data
    """
    csv_path = _get_data_path("dim_date.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'date_id', 'date_value', 'year', 'quarter', 'month', 'month_name',
            'day', 'day_of_week', 'day_name', 'week_of_year', 'is_weekend',
            'is_holiday', 'fiscal_year', 'fiscal_quarter'
        ])


@model(
    name="src_dim_time",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract time dimension reference data",
    tags=["source", "extract", "dimension", "time"]
)
def src_dim_time():
    """
    Extract time dimension data from source CSV.

    The time dimension contains one row per minute of the day
    with various time attributes for analysis.

    Returns:
        pandas.DataFrame: Time dimension data
    """
    csv_path = _get_data_path("dim_time.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'time_id', 'time_value', 'hour', 'minute', 'second',
            'am_pm', 'market_hours'
        ])
