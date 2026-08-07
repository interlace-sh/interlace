-- A seed model: inline rows, so the project runs with no external source. Other
-- models reference it by name (`raw_events`) and interlace infers the dependency,
-- rewriting it to the physical table at apply time.
SELECT event_id, user_id, kind, CAST(amount AS DOUBLE) AS amount, country, ts
FROM (
    VALUES
        (1,  101, 'view',     0.00,  'US', TIMESTAMP '2026-01-01 09:00:00'),
        (2,  101, 'click',    0.00,  'US', TIMESTAMP '2026-01-01 09:01:00'),
        (3,  101, 'purchase', 49.90, 'US', TIMESTAMP '2026-01-01 09:03:00'),
        (4,  102, 'view',     0.00,  'GB', TIMESTAMP '2026-01-01 10:15:00'),
        (5,  102, 'view',     0.00,  'GB', TIMESTAMP '2026-01-01 10:16:00'),
        (6,  103, 'click',    0.00,  'DE', TIMESTAMP '2026-01-01 11:20:00'),
        (7,  103, 'purchase', 129.00, 'DE', TIMESTAMP '2026-01-01 11:25:00'),
        (8,  104, 'view',     0.00,  'US', TIMESTAMP '2026-01-01 12:05:00'),
        (9,  104, 'click',    0.00,  'US', TIMESTAMP '2026-01-01 12:06:00'),
        (10, 104, 'purchase', 19.99, 'US', TIMESTAMP '2026-01-01 12:09:00'),
        (11, 105, 'view',     0.00,  'GB', TIMESTAMP '2026-01-01 13:30:00'),
        (12, 105, 'purchase', 74.50, 'GB', TIMESTAMP '2026-01-01 13:33:00')
) AS t (event_id, user_id, kind, amount, country, ts)
