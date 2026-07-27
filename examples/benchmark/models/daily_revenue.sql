/* interlace:
  strategy: incremental_by_time
  time_column: day
  interval: 1d
  checks:
    - not_null: day
*/
-- Incremental: each backfill task fills ONE day window (delete+insert on the
-- window), and the interval ledger remembers which days are done.
--
--   interlace run --start 2026-06-01 --end 2026-07-01     # 30 window tasks
--   interlace run --start 2026-06-01 --end 2026-07-01     # 0 tasks: ledger says done
--   interlace restate --start 2026-06-08 --end 2026-06-15 # rewrite one week
SELECT CAST(ts AS DATE) AS day, count(*) AS events, sum(amount) AS revenue
FROM events
GROUP BY day
