-- TPC-H Query 6: Forecasting Revenue Change
-- @name: tpch_q06
-- @schema: main
-- @materialize: ephemeral
-- @dependencies: tpch_data

SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= CAST('1994-01-01' AS date)
    AND l_shipdate < CAST('1995-01-01' AS date)
    AND l_discount BETWEEN 0.05
    AND 0.07
    AND l_quantity < 24;


