/* interlace:
  materialise: table
  target: ext.main.crm_snapshot
  strategy: replace
*/
-- table + full: reverse ETL into an external table. `full` here means DELETE all
-- + INSERT in place (ReplaceInPlace) — the live table is never dropped, so grants
-- and readers survive. Terminal models are environment-gated (default: prod only).
SELECT id, name, tier FROM customers
