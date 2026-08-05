/* interlace:
  materialise: table
  target: ext.main.crm_events
  strategy: incremental_by_time
  time_column: day
  interval: 1d
*/
-- table + incremental_by_time: windowed DELETE + INSERT straight into an external
-- table — a capability the old reverse-ETL sink could not express. `interlace
-- apply` fills the source's range; `run --start/--end` backfills or restates a
-- window against the destination, ledger-tracked like a virtual incremental.
SELECT id, day FROM customers
