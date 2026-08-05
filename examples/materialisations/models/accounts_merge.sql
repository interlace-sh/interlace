/* interlace: { strategy: merge, key: id } */
-- virtual + merge: keyed upsert into the owned snapshot table. Re-running
-- with new source rows deletes the matching keys and re-inserts — state
-- accumulates across runs under a stable definition.
SELECT id, name, tier FROM customers
