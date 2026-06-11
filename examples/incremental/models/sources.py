"""Source models — ingest raw data from CSV, APIs, and external systems.

Demonstrates: append strategy, caching (if_exists + TTL), retry policies.
"""

from interlace import get_connection, model
from interlace.core.retry.policy import API_RETRY_POLICY


@model(name="user_events", strategy="append")
def user_events():
    """Load user event stream from CSV. New rows appended on each run."""
    conn = get_connection()
    return conn.read_csv("data/user_events.csv")


@model(
    name="feature_flags",
    strategy="replace",
    cache={"strategy": "if_exists"},
    description="Feature flag configuration, cached until table is dropped",
)
def feature_flags():
    """Load feature flags. Cached: skips CSV read if the table already exists."""
    conn = get_connection()
    return conn.read_csv("data/feature_flags.csv")


@model(
    name="billing_data",
    strategy="replace",
    cache={"ttl": "6h"},
    retry_policy=API_RETRY_POLICY,
    description="Billing records from external API, cached for 6 hours with retry",
)
def billing_data():
    """Mock billing API. Returns hardcoded records; retries on transient failures."""
    return [
        {"invoice_id": 1, "user_id": 101, "amount": 49.99, "plan": "pro", "billed_at": "2024-01-15"},
        {"invoice_id": 2, "user_id": 102, "amount": 29.99, "plan": "starter", "billed_at": "2024-01-15"},
        {"invoice_id": 3, "user_id": 103, "amount": 99.99, "plan": "enterprise", "billed_at": "2024-02-01"},
        {"invoice_id": 4, "user_id": 104, "amount": 29.99, "plan": "starter", "billed_at": "2024-02-01"},
        {"invoice_id": 5, "user_id": 105, "amount": 49.99, "plan": "pro", "billed_at": "2024-02-15"},
        {"invoice_id": 6, "user_id": 101, "amount": 49.99, "plan": "pro", "billed_at": "2024-02-15"},
        {"invoice_id": 7, "user_id": 106, "amount": 29.99, "plan": "starter", "billed_at": "2024-03-01"},
        {"invoice_id": 8, "user_id": 107, "amount": 99.99, "plan": "enterprise", "billed_at": "2024-03-01"},
    ]
