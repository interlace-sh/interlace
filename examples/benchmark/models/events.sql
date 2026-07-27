-- The firehose: 25,000,000 synthetic events, generated in-engine (no files to
-- download). Deterministic — hash(id) drives every column — so reruns and
-- restatements produce identical data. Bump the range() to scale the benchmark.
SELECT
    r AS event_id,
    hash(r) % 100000 AS user_id,
    hash(r + 1) % 5000 AS product_id,
    CASE hash(r + 2) % 4 WHEN 0 THEN 'ios' WHEN 1 THEN 'android' WHEN 2 THEN 'web' ELSE 'tv' END AS device,
    round((hash(r + 3) % 9000) / 100.0 + 10, 2) AS amount,
    TIMESTAMP '2026-06-01 00:00:00' + INTERVAL (hash(r + 4) % 2592000) SECOND AS ts
FROM range(25000000) AS t (r)
