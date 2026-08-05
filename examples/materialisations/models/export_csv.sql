/* interlace:
  materialise: file
  format: csv
  path: out/customers.csv
*/
-- file + csv: same delivery, CSV with a header row.
SELECT id, name, tier FROM customers
