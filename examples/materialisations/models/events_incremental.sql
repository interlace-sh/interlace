/* interlace: { strategy: incremental, time_column: day, interval: 1d } */
-- virtual + incremental: processed one [start, end) grain at a time
-- (delete the window, insert the window). `interlace apply` fills the source's
-- whole range on first build; `interlace run --start/--end` backfills history.
SELECT id, name, day FROM customers
