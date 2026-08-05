/* interlace:
  materialise: table
  target: crm.main.customer_scores
  strategy: merge_by_key
  key: customer_id
  environments: [dev, prod]
*/
-- Reverse ETL: upsert scores into the attached CRM database. The live table is
-- never dropped; only changed keys are touched. Terminal deliveries are
-- environment-gated (default: prod only) so a sandbox apply never fires at a live
-- destination; this tour opts dev in because its "CRM" is a local file.
SELECT customer_id, name, score, NOW() as ts FROM customer_value
