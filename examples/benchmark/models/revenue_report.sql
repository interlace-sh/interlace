/* interlace:
  export: {to: parquet, path: out/daily_revenue.parquet}
*/
-- A file sink: the daily numbers land as Parquet under out/ on every build.
SELECT day, events, revenue
FROM daily_revenue
ORDER BY day
