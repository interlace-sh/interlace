/* interlace:
  materialise: view
*/
-- A view at the end of the product branch: promoted like any model, zero build cost.
SELECT product_id, sum(revenue) AS revenue
FROM by_product
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 20
