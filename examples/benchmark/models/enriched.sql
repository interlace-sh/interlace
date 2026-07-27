/* interlace:
  materialise: ephemeral
*/
-- Ephemeral: no table is built — this query is inlined as a CTE into every
-- consumer, so each of the four branch aggregates below scans the full 25M rows
-- through it. That is the point: the fan-out does real, repeated work.
SELECT
    event_id,
    user_id,
    product_id,
    device,
    amount,
    ts,
    CAST(ts AS DATE) AS day,
    CASE WHEN amount >= 80 THEN 'high' WHEN amount >= 40 THEN 'mid' ELSE 'low' END AS band
FROM events
