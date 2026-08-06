/* interlace:
  strategy: scd
  key: user_id
*/
-- Slowly-changing dimension (Type 2) over the 100k-user rollup: each run compares
-- the source against the open rows and version-stamps only what changed, adding
-- _valid_from / _valid_to. The first build opens a row per user; re-run after a
-- tier change and the old version is closed and the new one opened — full history,
-- no row hashing. Column-agnostic, so it runs on every engine.
SELECT
    user_id,
    spend,
    CASE WHEN spend >= 5000 THEN 'gold' WHEN spend >= 1000 THEN 'silver' ELSE 'bronze' END AS tier
FROM by_user
