-- TPC-DS Query 62
-- @name: tpcds_q62
-- @schema: tpcds
-- @materialize: table
-- @strategy: replace
-- @dependencies: tpcds_data
-- @fields: {"w_substr": "str", "sm_type": "str", "web_name": "str", "30 days": "float", "31-60 days": "float", "61-90 days": "float", "91-120 days": "float", ">120 days": "float"}

SELECT w_substr,
       sm_type,
       web_name,
       sum(CASE
               WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1
               ELSE 0
           END) AS "30 days",
       sum(CASE
               WHEN (ws_ship_date_sk - ws_sold_date_sk > 30)
                    AND (ws_ship_date_sk - ws_sold_date_sk <= 60) THEN 1
               ELSE 0
           END) AS "31-60 days",
       sum(CASE
               WHEN (ws_ship_date_sk - ws_sold_date_sk > 60)
                    AND (ws_ship_date_sk - ws_sold_date_sk <= 90) THEN 1
               ELSE 0
           END) AS "61-90 days",
       sum(CASE
               WHEN (ws_ship_date_sk - ws_sold_date_sk > 90)
                    AND (ws_ship_date_sk - ws_sold_date_sk <= 120) THEN 1
               ELSE 0
           END) AS "91-120 days",
       sum(CASE
               WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1
               ELSE 0
           END) AS ">120 days"
FROM web_sales,
  (SELECT SUBSTRING(w_warehouse_name,1,20) w_substr,
          *
   FROM warehouse) sq1,
     ship_mode,
     web_site,
     date_dim
WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
  AND ws_ship_date_sk = d_date_sk
  AND ws_warehouse_sk = w_warehouse_sk
  AND ws_ship_mode_sk = sm_ship_mode_sk
  AND ws_web_site_sk = web_site_sk
GROUP BY w_substr,
         sm_type,
         web_name
ORDER BY 1 NULLS FIRST,
         2 NULLS FIRST,
         3 NULLS FIRST
LIMIT 100;
