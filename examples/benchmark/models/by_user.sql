/* interlace:
  checks:
    - row_count: {min: 90000}
*/
-- Branch 1 of 4. The four by_* models share no edges, so `apply` builds them
-- CONCURRENTLY — watch the progress rows overlap.
SELECT user_id, count(*) AS events, sum(amount) AS spend, max(ts) AS last_seen
FROM enriched
GROUP BY user_id
