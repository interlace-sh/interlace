"""
TPC-DI Staging Models: Customer Data Transformations

Transforms raw customer, account, and broker data with:
- Data cleansing (standardization, trimming)
- Data enrichment (derived fields)
- Data validation (filtering invalid records)

All transformations use Ibis expressions for lazy evaluation.
"""

from interlace import model
import ibis


@model(
    name="stg_customer",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Staged customer data with cleansing and enrichment",
    tags=["staging", "transform", "customer"]
)
def stg_customer(src_customer: ibis.Table) -> ibis.Table:
    """
    Transform raw customer data for warehouse loading.

    Transformations:
    - Standardize email to lowercase
    - Standardize state/country to uppercase
    - Create full_name derived field
    - Filter out records with null customer_id

    Args:
        src_customer: Raw customer source data

    Returns:
        ibis.Table: Cleansed and enriched customer data
    """
    return (
        src_customer
        # Data validation - filter out invalid records
        .filter(src_customer.customer_id.notnull())
        # Data cleansing and enrichment
        .mutate(
            # Standardize text fields
            email=src_customer.email.lower(),
            state=src_customer.state.upper(),
            country=src_customer.country.upper(),
            # Derived field: full name (string concatenation with +)
            full_name=src_customer.first_name + ' ' + src_customer.last_name,
            # Clean up tier to ensure integer
            tier=src_customer.tier.cast('int64'),
        )
        .select([
            'customer_id', 'tax_id', 'first_name', 'last_name', 'full_name',
            'gender', 'tier', 'dob', 'email', 'phone',
            'address_line1', 'address_line2', 'city', 'state', 'postal_code',
            'country', 'national_id', 'effective_date', 'end_date', 'is_current'
        ])
    )


@model(
    name="stg_account",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Staged account data with cleansing and validation",
    tags=["staging", "transform", "account"]
)
def stg_account(src_account: ibis.Table) -> ibis.Table:
    """
    Transform raw account data for warehouse loading.

    Transformations:
    - Standardize account_type and status to uppercase
    - Validate required fields
    - Filter out records with null account_id

    Args:
        src_account: Raw account source data

    Returns:
        ibis.Table: Cleansed account data
    """
    return (
        src_account
        # Data validation
        .filter(src_account.account_id.notnull())
        .filter(src_account.customer_id.notnull())
        # Data cleansing
        .mutate(
            account_type=src_account.account_type.upper(),
            status=src_account.status.upper(),
            tax_status=src_account.tax_status.upper(),
        )
        .select([
            'account_id', 'customer_id', 'broker_id', 'account_type',
            'status', 'open_date', 'close_date', 'tax_status'
        ])
    )


@model(
    name="stg_broker",
    schema="staging",
    materialise="table",
    strategy="replace",
    description="Staged broker data with cleansing and enrichment",
    tags=["staging", "transform", "broker"]
)
def stg_broker(src_broker: ibis.Table) -> ibis.Table:
    """
    Transform raw broker data for warehouse loading.

    Transformations:
    - Standardize email to lowercase
    - Create full_name derived field
    - Standardize status to uppercase
    - Filter out records with null broker_id

    Args:
        src_broker: Raw broker source data

    Returns:
        ibis.Table: Cleansed and enriched broker data
    """
    return (
        src_broker
        # Data validation
        .filter(src_broker.broker_id.notnull())
        # Data cleansing and enrichment
        .mutate(
            email=src_broker.email.lower(),
            status=src_broker.status.upper(),
            full_name=src_broker.first_name + ' ' + src_broker.last_name,
        )
        .select([
            'broker_id', 'first_name', 'last_name', 'full_name',
            'email', 'phone', 'branch', 'commission_rate',
            'hire_date', 'status'
        ])
    )
