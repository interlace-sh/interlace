/* interlace:
  checks:            # data-quality gates: an error-severity failure blocks promotion
    - not_null: country
    - row_count: {min: 1}
*/
-- Reads the Python model's output (`enriched_events`) — referenced by name, like
-- any model. Per-country conversions and revenue.
SELECT
    country,
    count(*) AS events,
    count(*) FILTER (WHERE is_conversion) AS conversions,
    round(sum(revenue), 2) AS revenue
FROM enriched_events
GROUP BY country
ORDER BY revenue DESC
