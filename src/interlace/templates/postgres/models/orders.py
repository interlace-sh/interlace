"""Pull orders from a source Postgres into the warehouse — incrementally.

A *source* model: an ordinary ``@model`` that reads from an external system and
yields Arrow. Postgres isn't HTTP, so this uses ``psycopg`` directly rather than the
REST client — but the incremental contract is identical:

- ``@model(cursor="updated_at")`` injects the max ``updated_at`` already loaded (or
  ``None`` on the first build); we pass it to a ``WHERE updated_at > %s`` filter, so
  each run reads only rows changed since last time.
- ``strategy="merge", key="id"`` upserts by primary key, so re-reading the boundary
  is idempotent.

The bundled ``docker-compose.yml`` seeds a source database; ``SOURCE_PG_DSN`` overrides
the connection to point at your own. Rows stream in batches, so the pull stays
memory-bounded however large the table.

Requires the postgres extra:  pip install "interlaced[postgres]"
"""

import os
from datetime import datetime, timezone

import pyarrow as pa

from interlace import model

DSN = os.environ.get("SOURCE_PG_DSN", "postgresql://interlace:interlace@localhost:5456/shop")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_BATCH = 10_000

SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("customer", pa.string()),
        ("amount", pa.float64()),
        ("status", pa.string()),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)


@model(cursor="updated_at", strategy="merge", key="id")
def orders(cursor=None):
    import psycopg  # imported here so the project compiles without the postgres extra

    since = cursor or _EPOCH
    query = (
        "SELECT id, customer, amount::float8 AS amount, status, updated_at "
        "FROM orders WHERE updated_at > %s ORDER BY updated_at"
    )
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(query, (since,))
        names = [column.name for column in cur.description]
        while rows := cur.fetchmany(_BATCH):
            yield pa.RecordBatch.from_pylist([dict(zip(names, row)) for row in rows], schema=SCHEMA)
