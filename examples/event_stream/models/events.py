"""A durable event-ingestion stream: POST /streams/events on the daemon.

Every publish is fsynced to the durable log before the 200 returns, deduplicated
by event_id on retries, and materialized exactly-once into `streams.events` —
where the SQL models in this project read it. `generate.py` is the external
producer that fires events at the endpoint.
"""

from interlace import stream


@stream(
    "events",
    schema={
        "event_id": "string",
        "user_id": "integer",
        "event_type": "string",  # view / click / add_to_cart / purchase
        "amount": "double",  # revenue on a purchase, else 0
        "ts": "timestamp",
    },
    idempotency_key="event_id",  # a re-POSTed event_id deduplicates, never double-counts
    retention="1h",  # the durable log trims events older than this
    on_schema_drift="reject",  # unknown fields / wrong types are refused at publish time
)
def events(event):
    return event
