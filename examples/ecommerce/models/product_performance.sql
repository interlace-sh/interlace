-- @name: product_performance
-- @strategy: replace
-- @tags: analytics, sql
-- @description: Product performance metrics computed in SQL

SELECT
    p.product_id,
    p.name AS product_name,
    p.category,
    p.price,
    p.cost,
    COUNT(DISTINCT o.order_id) AS times_ordered,
    SUM(o.quantity) AS total_units_sold,
    SUM(o.quantity * p.price) AS total_revenue,
    SUM(o.quantity * (p.price - p.cost)) AS total_profit,
    ROUND(SUM(o.quantity * (p.price - p.cost)) / NULLIF(SUM(o.quantity * p.price), 0) * 100, 1) AS profit_margin_pct
FROM products p
LEFT JOIN stg_orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.name, p.category, p.price, p.cost
ORDER BY total_revenue DESC
