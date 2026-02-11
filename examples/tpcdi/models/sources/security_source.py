"""
TPC-DI Source Models: Security and Company Data Extraction

Extracts company and security master data from source CSV files.
"""

from interlace import model
from pathlib import Path
import pandas as pd


def _get_data_path(filename: str) -> str:
    """Get path to source data file."""
    project_root = Path(__file__).parent.parent.parent
    return str(project_root / "data" / "raw" / filename)


@model(
    name="src_company",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract company master data from source system",
    tags=["source", "extract", "company"]
)
def src_company():
    """
    Extract company master data from source CSV.

    Company records represent publicly traded companies
    whose securities can be bought and sold.

    Returns:
        pandas.DataFrame: Raw company data
    """
    csv_path = _get_data_path("company.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'company_id', 'name', 'industry', 'ceo', 'description',
            'founded_date', 'address', 'city', 'state', 'country',
            'employees', 'status'
        ])


@model(
    name="src_security",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract security master data from source system",
    tags=["source", "extract", "security"]
)
def src_security():
    """
    Extract security/stock master data from source CSV.

    Security records represent tradeable financial instruments
    (stocks, bonds, etc.) linked to companies.

    Returns:
        pandas.DataFrame: Raw security data
    """
    csv_path = _get_data_path("security.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'security_id', 'symbol', 'name', 'company_id', 'exchange',
            'issue_date', 'shares_outstanding', 'first_trade_date',
            'dividend', 'status'
        ])
