-- A seed model: inline rows, so the example runs with no external source.
SELECT *
FROM (
    VALUES
        (1, 'click', 100),
        (2, 'view', 50),
        (3, 'click', 75),
        (4, 'purchase', 300),
        (5, 'view', 25)
) AS t (event_id, kind, amount)
