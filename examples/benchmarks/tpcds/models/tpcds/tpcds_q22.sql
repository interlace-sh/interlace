-- TPC-DS Query 22
-- @name: tpcds_q22
-- @schema: tpcds
-- @materialize: table
-- @strategy: replace
-- @dependencies: tpcds_data
-- @fields: {"i_product_name": "str", "i_brand": "str", "i_class": "str", "i_category": "str", "qoh": "float"}

SELECT i_product_name ,
       i_brand ,
       i_class ,
       i_category ,
       avg(inv_quantity_on_hand) qoh
FROM inventory ,
     date_dim ,
     item
WHERE inv_date_sk=d_date_sk
  AND inv_item_sk=i_item_sk
  AND d_month_seq BETWEEN 1200 AND 1200 + 11
GROUP BY rollup(i_product_name ,i_brand ,i_class ,i_category)
ORDER BY qoh NULLS FIRST,
         i_product_name NULLS FIRST,
         i_brand NULLS FIRST,
         i_class NULLS FIRST,
         i_category NULLS FIRST
LIMIT 100;
