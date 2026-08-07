/* interlace: { strategy: hash_merge, key: id } */
-- virtual + hash_merge: keyed upsert that stores an _hash (md5 of the non-key
-- columns) and writes only the delta — a changed row updates, a new key inserts,
-- an unchanged row is skipped. Idempotent (a run over identical data writes nothing)
-- and its reported counts split cleanly into +inserted / ~updated. Unlike full_merge
-- it keeps keys absent from the source (an upsert, not a full-state sync).
SELECT id, name, tier FROM customers
