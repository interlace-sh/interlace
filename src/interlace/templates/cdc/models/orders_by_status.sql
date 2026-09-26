/* interlace:
  checks:
    - not_null: status
    - accepted_values: {column: status, values: [paid, refunded, pending]}
*/
-- Rollup over the replica. A flush of streams.orders rebuilds `orders`, then
-- this. Revenue counts paid orders only.
SELECT
    status,
    count(*) AS orders,
    round(coalesce(sum(amount) FILTER (WHERE status = 'paid'), 0), 2) AS revenue
FROM orders
GROUP BY status
ORDER BY orders DESC
