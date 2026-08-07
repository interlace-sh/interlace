"""A Python model in the middle of the pipeline.

Its parameter is named after the `raw_events` model, so interlace infers the
dependency — no `depends_on` needed. Data arrives and leaves as Arrow (never
pandas), a batch at a time, so memory stays bounded however large the source grows.
Here it derives a `revenue` column (the amount on a purchase, else 0) and an
`is_conversion` flag — the kind of row-wise logic that is clumsy in SQL.
"""

import pyarrow as pa
import pyarrow.compute as pc

from interlace import model


@model()  # materialise: virtual (default), strategy: replace (default)
def enriched_events(raw_events):
    for batch in raw_events.reader():
        is_conversion = pc.equal(batch.column("kind"), "purchase")
        revenue = pc.if_else(is_conversion, batch.column("amount"), pa.scalar(0.0))
        columns = [*batch.columns, revenue, is_conversion]
        names = [*batch.schema.names, "revenue", "is_conversion"]
        yield pa.RecordBatch.from_arrays(columns, names=names)
