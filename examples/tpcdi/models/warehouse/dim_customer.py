"""
TPC-DI Warehouse Models: Dimension Tables

Loads transformed data into data warehouse dimension tables.
Uses merge_by_key strategy for SCD Type 1 updates.

Note: True SCD Type 2 (historical tracking) would require
additional Interlace features. This implementation uses
merge_by_key which performs upserts on the primary key.
"""

from interlace import model
import ibis


@model(
    name="dim_customer",
    schema="warehouse",
    materialise="table",
    strategy="merge_by_key",
    primary_key="customer_id",
    description="Customer dimension table with SCD Type 2 tracking",
    tags=["warehouse", "dimension", "customer"]
)
def dim_customer(stg_customer: ibis.Table) -> ibis.Table:
    """
    Load customer dimension table.

    This represents the customer dimension in the data warehouse.
    Uses merge_by_key strategy for incremental updates.

    The source data includes SCD Type 2 fields:
    - effective_date: When this version became active
    - end_date: When superseded (9999-12-31 = current)
    - is_current: Flag for current record

    Args:
        stg_customer: Staged customer data

    Returns:
        ibis.Table: Customer dimension table
    """
    return stg_customer.select([
        'customer_id', 'tax_id', 'first_name', 'last_name', 'full_name',
        'gender', 'tier', 'dob', 'email', 'phone',
        'address_line1', 'address_line2', 'city', 'state', 'postal_code',
        'country', 'national_id', 'effective_date', 'end_date', 'is_current'
    ])


@model(
    name="dim_account",
    schema="warehouse",
    materialise="table",
    strategy="merge_by_key",
    primary_key="account_id",
    description="Account dimension table",
    tags=["warehouse", "dimension", "account"]
)
def dim_account(stg_account: ibis.Table) -> ibis.Table:
    """
    Load account dimension table.

    Accounts link customers to brokers and define
    account type and tax treatment.

    Args:
        stg_account: Staged account data

    Returns:
        ibis.Table: Account dimension table
    """
    return stg_account.select([
        'account_id', 'customer_id', 'broker_id', 'account_type',
        'status', 'open_date', 'close_date', 'tax_status'
    ])


@model(
    name="dim_broker",
    schema="warehouse",
    materialise="table",
    strategy="merge_by_key",
    primary_key="broker_id",
    description="Broker dimension table",
    tags=["warehouse", "dimension", "broker"]
)
def dim_broker(stg_broker: ibis.Table) -> ibis.Table:
    """
    Load broker dimension table.

    Brokers are financial advisors who manage customer accounts.

    Args:
        stg_broker: Staged broker data

    Returns:
        ibis.Table: Broker dimension table
    """
    return stg_broker.select([
        'broker_id', 'first_name', 'last_name', 'full_name',
        'email', 'phone', 'branch', 'commission_rate',
        'hire_date', 'status'
    ])


@model(
    name="dim_company",
    schema="warehouse",
    materialise="table",
    strategy="merge_by_key",
    primary_key="company_id",
    description="Company dimension table",
    tags=["warehouse", "dimension", "company"]
)
def dim_company(stg_company: ibis.Table) -> ibis.Table:
    """
    Load company dimension table.

    Companies are publicly traded entities whose securities
    can be bought and sold.

    Args:
        stg_company: Staged company data

    Returns:
        ibis.Table: Company dimension table
    """
    return stg_company.select([
        'company_id', 'name', 'industry', 'ceo', 'description',
        'founded_date', 'address', 'city', 'state', 'country',
        'employees', 'status'
    ])


@model(
    name="dim_security",
    schema="warehouse",
    materialise="table",
    strategy="merge_by_key",
    primary_key="security_id",
    description="Security dimension table",
    tags=["warehouse", "dimension", "security"]
)
def dim_security(stg_security: ibis.Table) -> ibis.Table:
    """
    Load security dimension table.

    Securities are tradeable financial instruments (stocks, bonds, etc.)

    Args:
        stg_security: Staged security data

    Returns:
        ibis.Table: Security dimension table
    """
    return stg_security.select([
        'security_id', 'symbol', 'name', 'company_id', 'exchange',
        'issue_date', 'shares_outstanding', 'first_trade_date',
        'dividend', 'status'
    ])


@model(
    name="dim_date",
    schema="warehouse",
    materialise="table",
    strategy="replace",
    description="Date dimension table (reference data)",
    tags=["warehouse", "dimension", "date", "reference"]
)
def dim_date(src_dim_date: ibis.Table) -> ibis.Table:
    """
    Load date dimension table.

    Pre-generated reference table with one row per calendar day.
    Uses replace strategy as this is static reference data.

    Args:
        src_dim_date: Source date dimension data

    Returns:
        ibis.Table: Date dimension table
    """
    return src_dim_date.select([
        'date_id', 'date_value', 'year', 'quarter', 'month', 'month_name',
        'day', 'day_of_week', 'day_name', 'week_of_year', 'is_weekend',
        'is_holiday', 'fiscal_year', 'fiscal_quarter'
    ])


@model(
    name="dim_time",
    schema="warehouse",
    materialise="table",
    strategy="replace",
    description="Time dimension table (reference data)",
    tags=["warehouse", "dimension", "time", "reference"]
)
def dim_time(src_dim_time: ibis.Table) -> ibis.Table:
    """
    Load time dimension table.

    Pre-generated reference table with one row per minute.
    Uses replace strategy as this is static reference data.

    Args:
        src_dim_time: Source time dimension data

    Returns:
        ibis.Table: Time dimension table
    """
    return src_dim_time.select([
        'time_id', 'time_value', 'hour', 'minute', 'second',
        'am_pm', 'market_hours'
    ])
