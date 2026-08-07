/* interlace:
  schedule: {every: 1m}
*/
-- Live per-minute rollup over the durable stream. Each flush re-triggers this,
-- and the scheduler runs it every minute regardless — so the series stays current
-- whether or not events are flowing.
SELECT
    date_trunc('minute', ts) AS minute,
    count(*) AS events,
    count(DISTINCT user_id) AS users,
    round(sum(amount), 2) AS revenue
FROM streams.events
GROUP BY 1
ORDER BY minute
