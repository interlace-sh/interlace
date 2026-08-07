/* interlace:
  materialise: view
*/
-- A view at the tip of the DAG: the current biggest spenders, recomputed on read
-- (zero build cost, promoted like any model).
SELECT user_id, spend, events
FROM user_spend
ORDER BY spend DESC
LIMIT 20
