-- Branch 4 of 4: daily totals (full-refresh flavour; daily_revenue is the
-- incremental flavour of the same shape).
SELECT day, count(*) AS events, sum(amount) AS revenue
FROM enriched
GROUP BY day
