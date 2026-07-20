"""A Python model: Arrow in, Arrow out — parameters name the upstream models."""

import pyarrow as pa
import pyarrow.compute as pc

from interlace import model


@model(depends_on=["dim_customers"], columns=["customer_id", "name", "score"])
def customer_value(dim_customers):
    current = dim_customers.table().filter(pc.field("_valid_to").is_null())
    scores = pc.multiply(pc.index_in(current["tier"], pa.array(["bronze", "silver", "gold"])), 10)
    return pa.table({"customer_id": current["customer_id"], "name": current["name"], "score": scores})
