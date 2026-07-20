-- A seed source: inline rows so the tour runs with no external dependencies.
SELECT *
FROM (
    VALUES
        (1, 'ada', 'gold'),
        (2, 'bob', 'silver'),
        (3, 'cli', 'bronze')
) AS t (customer_id, name, tier)
