/* interlace:
  export: {to: table, target: crm.main.customer_scores, mode: merge_by_key, key: customer_id}
*/
-- Reverse ETL: upsert scores into the attached CRM database. The live table is
-- never dropped; only changed keys are touched.
SELECT customer_id, name, score, NOW() as ts FROM customer_value
