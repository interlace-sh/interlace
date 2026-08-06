/* interlace:
  materialise: table
  target: serving.main.daily_feed
  strategy: append
*/
-- Reverse ETL: append the daily rollup into an external serving database
-- (`serving`, attached in interlace.yaml) — a table interlace delivers into but
-- never owns, so grants and readers on it survive. append adds rows and deletes
-- nothing; it is environment-gated, so it only fires against prod.
SELECT day, events, revenue
FROM daily_revenue
