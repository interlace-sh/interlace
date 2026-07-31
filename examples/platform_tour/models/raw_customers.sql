-- A seed source: inline rows so the tour runs with no external dependencies.
SELECT *
FROM (
    VALUES
        (1, 'ada', 'gold', true),
        (2, 'bob', 'silver', true),
        (3, 'cli', 'bronze', true),
        (4, 'qwerty', 'bronze', false)
) AS t (customer_id, name, tier, enabled)
