/* interlace:
  strategy: full_merge
  key: [product_id, band]
*/
-- Full-state sync with a composite key: by_product is the complete current
-- catalog every run, so full_merge applies only the difference (EXCEPT is the
-- row hash — no column list, no watermark). New and changed rows are inserted,
-- keys that vanished upstream are deleted, and unchanged rows are left untouched
-- (an identical rerun writes nothing — no new DuckLake files).
SELECT product_id, band, events, revenue
FROM by_product
