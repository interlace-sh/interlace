/* interlace:
  materialise: table
  target: ext.main.crm_accounts
  strategy: merge
  key: id
*/
-- table + merge: keyed upsert into an external table — the SAME strategy
-- as a virtual merge, pointed at the destination. Changed keys are replaced,
-- untouched rows are left alone, and the table is never dropped.
SELECT id, name, tier FROM customers
