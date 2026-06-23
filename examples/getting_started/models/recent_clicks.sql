/*
interlace:
  materialise: view
*/
-- A view over raw_events (per-model config selects the view materialisation).
SELECT event_id, amount
FROM raw_events
WHERE kind = 'click'
