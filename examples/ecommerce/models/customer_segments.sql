-- @name: customer_segments
-- @strategy: replace
-- @tags: analytics, sql
-- @description: Customer segmentation using window functions

SELECT
    c.customer_id,
    c.name,
    c.segment,
    COALESCE(stats.total_spent, 0) AS total_spent,
    COALESCE(stats.order_count, 0) AS order_count,
    NTILE(4) OVER (ORDER BY COALESCE(stats.total_spent, 0) DESC) AS spend_quartile,
    CASE
        WHEN stats.order_count >= 8 THEN 'champion'
        WHEN stats.order_count >= 5 THEN 'loyal'
        WHEN stats.order_count >= 2 THEN 'regular'
        WHEN stats.order_count >= 1 THEN 'new'
        ELSE 'inactive'
    END AS rfm_segment
FROM customers c
LEFT JOIN (
    SELECT
        op.customer_id,
        SUM(op.amount) AS total_spent,
        COUNT(DISTINCT op.order_id) AS order_count
    FROM order_payments op
    GROUP BY op.customer_id
) stats ON c.customer_id = stats.customer_id
ORDER BY total_spent DESC
