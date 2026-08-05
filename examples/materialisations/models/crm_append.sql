/* interlace:
  materialise: table
  target: ext.main.crm_log
  strategy: append
  environments: [dev, prod]
*/
-- table + append: add the query's rows to an external log table (never deletes).
-- `append` is a terminal-only strategy. environments: [dev, prod] opts this
-- delivery into dev too (the default is prod only).
SELECT id, name FROM customers
