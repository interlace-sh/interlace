-- Branch 2 of 4: product rollup with a band breakdown.
SELECT product_id, band, count(*) AS events, sum(amount) AS revenue
FROM enriched
GROUP BY product_id, band
