/* interlace: { strategy: incremental_by_time, time_column: day, interval: 1d } */
-- virtual + incremental_by_time: processed one [start, end) grain at a time
-- (delete the window, insert the window). `interlace apply` fills the source's
-- whole range on first build; `interlace run --start/--end` backfills history.
SELECT id, name, day FROM customers
