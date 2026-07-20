/* interlace:
  schedule: {every: 5m}
  checks:
    - expression: {expression: "revenue >= 0", severity: warn}
*/
-- Reads the materialized stream directly; a stream flush re-triggers this model.
SELECT
    count(*) AS orders,
    coalesce(sum(total), 0) AS revenue
FROM streams.orders
