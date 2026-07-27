"""A Python model in the hot path: 100k user rows stream through Arrow.

The generator yields RecordBatches, so memory stays bounded no matter how far
you scale events.sql; merge_by_key then upserts the output in SQL.
"""

import pyarrow as pa
import pyarrow.compute as pc

from interlace import model


@model(depends_on=["by_user"], strategy="merge_by_key", key=["user_id"])
def user_ltv(by_user):
    for batch in by_user.reader():
        score = pc.add(pc.multiply(batch.column("spend"), 0.1), batch.column("events"))
        yield pa.RecordBatch.from_arrays(
            [batch.column("user_id"), batch.column("spend"), pc.round(score, 2)],
            names=["user_id", "spend", "ltv"],
        )
