-- TPC-DS Query 28
-- @name: tpcds_q28
-- @schema: tpcds
-- @materialize: table
-- @strategy: replace
-- @dependencies: tpcds_data
-- @fields: {"B1_LP": "float", "B1_CNT": "int", "B1_CNTD": "int", "B2_LP": "float", "B2_CNT": "int", "B2_CNTD": "int", "B3_LP": "float", "B3_CNT": "int", "B3_CNTD": "int", "B4_LP": "float", "B4_CNT": "int", "B4_CNTD": "int", "B5_LP": "float", "B5_CNT": "int", "B5_CNTD": "int", "B6_LP": "float", "B6_CNT": "int", "B6_CNTD": "int"}

SELECT *
FROM
  (SELECT avg(ss_list_price) B1_LP,
          count(ss_list_price) B1_CNT,
          count(DISTINCT ss_list_price) B1_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 0 AND 5
     AND (ss_list_price BETWEEN 8 AND 8+10
          OR ss_coupon_amt BETWEEN 459 AND 459+1000
          OR ss_wholesale_cost BETWEEN 57 AND 57+20)) B1,
  (SELECT avg(ss_list_price) B2_LP,
          count(ss_list_price) B2_CNT,
          count(DISTINCT ss_list_price) B2_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 6 AND 10
     AND (ss_list_price BETWEEN 90 AND 90+10
          OR ss_coupon_amt BETWEEN 2323 AND 2323+1000
          OR ss_wholesale_cost BETWEEN 31 AND 31+20)) B2,
  (SELECT avg(ss_list_price) B3_LP,
          count(ss_list_price) B3_CNT,
          count(DISTINCT ss_list_price) B3_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 11 AND 15
     AND (ss_list_price BETWEEN 142 AND 142+10
          OR ss_coupon_amt BETWEEN 12214 AND 12214+1000
          OR ss_wholesale_cost BETWEEN 79 AND 79+20)) B3,
  (SELECT avg(ss_list_price) B4_LP,
          count(ss_list_price) B4_CNT,
          count(DISTINCT ss_list_price) B4_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 16 AND 20
     AND (ss_list_price BETWEEN 135 AND 135+10
          OR ss_coupon_amt BETWEEN 6071 AND 6071+1000
          OR ss_wholesale_cost BETWEEN 38 AND 38+20)) B4,
  (SELECT avg(ss_list_price) B5_LP,
          count(ss_list_price) B5_CNT,
          count(DISTINCT ss_list_price) B5_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 21 AND 25
     AND (ss_list_price BETWEEN 122 AND 122+10
          OR ss_coupon_amt BETWEEN 836 AND 836+1000
          OR ss_wholesale_cost BETWEEN 17 AND 17+20)) B5,
  (SELECT avg(ss_list_price) B6_LP,
          count(ss_list_price) B6_CNT,
          count(DISTINCT ss_list_price) B6_CNTD
   FROM store_sales
   WHERE ss_quantity BETWEEN 26 AND 30
     AND (ss_list_price BETWEEN 154 AND 154+10
          OR ss_coupon_amt BETWEEN 7326 AND 7326+1000
          OR ss_wholesale_cost BETWEEN 7 AND 7+20)) B6
LIMIT 100;
