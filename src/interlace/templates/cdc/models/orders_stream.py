"""CDC landing zone: `interlace serve` appends each Postgres change here.

pgoutput delivers every value as text, so the schema is strings. `_change` is
`insert`, `update`, or `delete`; `_table` is the source relation. The publisher
dedupes on the WAL LSN, so this declaration has no idempotency key of its own.
The body is not called — CDC writes the log directly.
"""

from interlace import stream


@stream(
    "orders",
    schema={
        "id": "string",
        "customer": "string",
        "amount": "string",
        "status": "string",
        "updated_at": "string",
        "_change": "string",
        "_table": "string",
    },
)
def orders(event):
    return event
