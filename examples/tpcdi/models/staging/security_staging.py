"""
TPC-DI Staging Models: Security and Company Transformations

Transforms raw company and security master data with:
- Data cleansing (standardization)
- Data enrichment (derived fields)
- Data validation (filtering invalid records)
"""

from interlace import model
import ibis


@model(
    name="stg_company",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Staged company data with cleansing",
    tags=["staging", "transform", "company"]
)
def stg_company(src_company: ibis.Table) -> ibis.Table:
    """
    Transform raw company data for warehouse loading.

    Transformations:
    - Standardize industry to title case
    - Standardize state/country to uppercase
    - Filter out records with null company_id

    Args:
        src_company: Raw company source data

    Returns:
        ibis.Table: Cleansed company data
    """
    return (
        src_company
        # Data validation
        .filter(src_company.company_id.notnull())
        # Data cleansing
        .mutate(
            state=src_company.state.upper(),
            country=src_company.country.upper(),
            status=src_company.status.upper(),
        )
        .select([
            'company_id', 'name', 'industry', 'ceo', 'description',
            'founded_date', 'address', 'city', 'state', 'country',
            'employees', 'status'
        ])
    )


@model(
    name="stg_security",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Staged security data with cleansing",
    tags=["staging", "transform", "security"]
)
def stg_security(src_security: ibis.Table) -> ibis.Table:
    """
    Transform raw security data for warehouse loading.

    Transformations:
    - Standardize symbol to uppercase
    - Standardize exchange to uppercase
    - Filter out records with null security_id

    Args:
        src_security: Raw security source data

    Returns:
        ibis.Table: Cleansed security data
    """
    return (
        src_security
        # Data validation
        .filter(src_security.security_id.notnull())
        # Data cleansing
        .mutate(
            symbol=src_security.symbol.upper(),
            exchange=src_security.exchange.upper(),
            status=src_security.status.upper(),
        )
        .select([
            'security_id', 'symbol', 'name', 'company_id', 'exchange',
            'issue_date', 'shares_outstanding', 'first_trade_date',
            'dividend', 'status'
        ])
    )
