-- Level 2: aggregate raw_events by kind. interlace resolves the `raw_events`
-- reference to its physical table automatically.
SELECT
    kind,
    count(*) AS events,
    sum(amount) AS total_amount
FROM raw_events
GROUP BY kind
