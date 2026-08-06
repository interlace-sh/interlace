-- Per-user activity and spend over the retained stream; feeds the top_users view.
SELECT
    user_id,
    count(*) AS events,
    round(sum(amount), 2) AS spend
FROM streams.events
GROUP BY user_id
