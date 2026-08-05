/* interlace:
  materialise: table
  target: ext.main.crm_state
  strategy: full_merge
  key: id
*/
-- table + full_merge: the query is the full desired state of the external table;
-- interlace applies only the diff (changed keys re-inserted, vanished keys
-- deleted) without ever dropping it.
SELECT id, name FROM customers
