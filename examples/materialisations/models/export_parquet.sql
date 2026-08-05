/* interlace:
  materialise: file
  format: parquet
  path: out/customers.parquet
*/
-- file + parquet: overwrite a file on every build via DuckDB COPY. No managed
-- table, no environment view; environment-gated like any terminal model.
SELECT id, name, tier, day FROM customers
