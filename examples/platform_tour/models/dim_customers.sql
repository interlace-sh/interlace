/* interlace:
  strategy: scd
  key: customer_id
  checks:
    - not_null: customer_id
*/
-- History-tracked dimension: change a tier in raw_customers and re-run —
-- the old version closes (_valid_to) and the new one opens.
SELECT customer_id, name, tier FROM raw_customers
