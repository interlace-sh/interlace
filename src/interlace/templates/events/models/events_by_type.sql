-- Breakdown by event type. No schedule needed: a stream flush re-triggers every
-- model that reads streams.events, so this refreshes as events land.
SELECT
    event_type,
    count(*) AS events,
    round(sum(amount), 2) AS revenue
FROM streams.events
GROUP BY event_type
ORDER BY events DESC
