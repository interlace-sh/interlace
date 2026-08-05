/* interlace: { materialise: ephemeral } */
-- ephemeral: no table is built — this query is inlined as a CTE into every
-- consumer below. Change it once and every downstream model re-derives.
SELECT * FROM (VALUES
    (1, 'ada',   'gold',   TIMESTAMP '2024-01-01 09:00'),
    (2, 'grace', 'silver', TIMESTAMP '2024-01-02 09:00'),
    (3, 'linus', 'gold',   TIMESTAMP '2024-01-03 09:00')
) AS t (id, name, tier, day)
