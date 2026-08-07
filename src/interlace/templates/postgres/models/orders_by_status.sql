/* interlace:
  checks:
    - not_null: status
    - accepted_values: {column: status, values: [paid, refunded, pending]}
*/
-- A plain SQL rollup over the pulled orders — proof the source lands as an ordinary
-- table you model on. Revenue counts paid orders only.
SELECT
    status,
    count(*) AS orders,
    round(sum(amount) FILTER (WHERE status = 'paid'), 2) AS revenue
FROM orders
GROUP BY status
ORDER BY orders DESC
