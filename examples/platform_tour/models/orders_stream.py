"""A durable ingestion stream: POST /streams/orders on the daemon.

Events are durable before the 200, deduplicated by order_id on retries, and
materialized exactly-once into streams.orders — where SQL models read them.
"""

from interlace import stream


@stream(
    "orders",
    schema={"order_id": "string", "customer_id": "integer", "total": "double"},
    idempotency_key="order_id",
    retention="7d",
)
def orders(event):
    return event
