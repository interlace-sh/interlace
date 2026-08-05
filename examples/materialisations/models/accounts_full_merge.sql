/* interlace: { strategy: full_merge, key: id } */
-- virtual + full_merge: the query is the complete desired state; only the
-- difference is applied (changed keys re-inserted, vanished keys deleted). A run
-- over identical data writes nothing.
SELECT id, name FROM customers
