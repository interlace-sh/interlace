"""
TPC-DI Source Models: Customer and Account Data Extraction

Extracts customer and account data from source CSV files.
These represent the "Extract" phase of the ETL pipeline.
"""

from interlace import model
from pathlib import Path
import pandas as pd


def _get_data_path(filename: str) -> str:
    """Get path to source data file."""
    project_root = Path(__file__).parent.parent.parent
    return str(project_root / "data" / "raw" / filename)


@model(
    name="src_customer",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract customer data from source system",
    tags=["source", "extract", "customer"]
)
def src_customer():
    """
    Extract customer data from source CSV.

    This is the raw customer data with SCD Type 2 tracking fields:
    - effective_date: When this version became active
    - end_date: When this version was superseded (9999-12-31 for current)
    - is_current: Flag indicating current record

    Returns:
        pandas.DataFrame: Raw customer data
    """
    csv_path = _get_data_path("customer.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'customer_id', 'tax_id', 'first_name', 'last_name', 'gender',
            'tier', 'dob', 'email', 'phone', 'address_line1', 'address_line2',
            'city', 'state', 'postal_code', 'country', 'national_id',
            'effective_date', 'end_date', 'is_current'
        ])


@model(
    name="src_account",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract account data from source system",
    tags=["source", "extract", "account"]
)
def src_account():
    """
    Extract account data from source CSV.

    Account records link customers to brokers and define
    the account type and tax treatment.

    Returns:
        pandas.DataFrame: Raw account data
    """
    csv_path = _get_data_path("account.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'account_id', 'customer_id', 'broker_id', 'account_type',
            'status', 'open_date', 'close_date', 'tax_status'
        ])


@model(
    name="src_broker",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Extract broker data from source system",
    tags=["source", "extract", "broker"]
)
def src_broker():
    """
    Extract broker/employee data from source CSV.

    Broker records represent financial advisors who manage
    customer accounts.

    Returns:
        pandas.DataFrame: Raw broker data
    """
    csv_path = _get_data_path("broker.csv")

    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=[
            'broker_id', 'first_name', 'last_name', 'email', 'phone',
            'branch', 'commission_rate', 'hire_date', 'status'
        ])
