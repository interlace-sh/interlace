-- SQL Models demonstrating SQL model syntax and dependencies

-- @name: product_revenue
-- @schema: public
-- @materialize: table
-- @strategy: replace
-- @dependencies: customer_orders, products

-- Calculate product revenue directly from source tables
-- (product_sales model may fail, so calculate directly)
SELECT 
    p.product_id,
    p.name,
    p.category,
    SUM(co.amount) AS total_revenue,
    SUM(co.quantity) AS total_quantity_sold,
    (SUM(co.amount) / NULLIF(SUM(co.quantity), 0)) AS avg_price_per_unit
FROM public.customer_orders co
JOIN public.products p ON co.product_id = p.product_id
GROUP BY p.product_id, p.name, p.category
ORDER BY total_revenue DESC;

-- @name: monthly_sales
-- @schema: public
-- @materialize: table
-- @strategy: replace
-- @dependencies: customer_orders

SELECT 
    DATE_TRUNC('month', CAST(order_date AS DATE)) AS month,
    COUNT(*) AS order_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value
FROM public.customer_orders
GROUP BY DATE_TRUNC('month', CAST(order_date AS DATE))
ORDER BY month DESC;

-- @name: customer_rankings
-- @schema: public
-- @materialize: view
-- @dependencies: customer_summary

SELECT 
    customer_id,
    total_orders,
    total_spent,
    avg_order_value,
    RANK() OVER (ORDER BY total_spent DESC) AS revenue_rank,
    RANK() OVER (ORDER BY total_orders DESC) AS order_count_rank
FROM public.customer_summary;

