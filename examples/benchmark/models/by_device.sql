-- Branch 3 of 4: device split.
SELECT device, count(*) AS events, sum(amount) AS revenue, avg(amount) AS avg_ticket
FROM enriched
GROUP BY device
