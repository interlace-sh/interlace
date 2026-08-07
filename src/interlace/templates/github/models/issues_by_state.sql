/* interlace:
  checks:
    - not_null: state
    - accepted_values: {column: state, values: [open, closed]}
*/
-- A plain SQL rollup over the pulled issues — proof the source lands as an ordinary
-- table you can model on. Open vs closed, and how much conversation each attracts.
SELECT
    state,
    count(*) AS issues,
    sum(comments) AS comments,
    round(avg(comments), 1) AS avg_comments
FROM github_issues
GROUP BY state
ORDER BY issues DESC
