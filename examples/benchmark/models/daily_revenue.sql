/* interlace:
  strategy: incremental
  time_column: day
  interval: 1d
  checks:
    - not_null: day
*/
-- Incremental: the FIRST build auto-backfills — apply derives the source's
-- time range (min/max of `day`) and fills it as one covering interval. After
-- that, each windowed run fills per-day windows and the interval ledger
-- remembers which days are done.
--
--   interlace apply                                       # bootstraps the whole June range
--   interlace run --start 2026-06-01 --end 2026-07-01     # 0 tasks: ledger says done
--   interlace restate --start 2026-06-08 --end 2026-06-15 # rewrite one week
--
-- Opt out with `backfill: none` (latest window only) or pin the start with
-- `backfill: "2026-06-15"`.
SELECT CAST(ts AS DATE) AS day, count(*) AS events, sum(amount) AS revenue
FROM events
GROUP BY day
