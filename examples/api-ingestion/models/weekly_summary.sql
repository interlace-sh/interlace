-- @name: weekly_summary
-- @strategy: replace
-- @tags: analytics, sql
-- @description: Weekly weather summary aggregation

SELECT
    DATE_TRUNC('week', CAST(date AS DATE)) AS week_start,
    COUNT(*) AS days_in_week,
    ROUND(MIN(temp_min_c), 1) AS week_temp_min_c,
    ROUND(MAX(temp_max_c), 1) AS week_temp_max_c,
    ROUND(AVG((temp_max_c + temp_min_c) / 2), 1) AS week_temp_avg_c,
    ROUND(AVG(precip_prob_pct), 0) AS avg_precip_prob_pct,
    ROUND(MAX(wind_max_kmh), 1) AS max_wind_kmh
FROM weather_forecast
GROUP BY DATE_TRUNC('week', CAST(date AS DATE))
ORDER BY week_start
