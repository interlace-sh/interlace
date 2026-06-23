-- Level 3: depends on event_totals, demonstrating a multi-level DAG.
SELECT kind, total_amount
FROM event_totals
ORDER BY total_amount DESC
LIMIT 1
