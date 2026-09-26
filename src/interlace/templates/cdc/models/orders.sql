/* interlace:
  strategy: full_merge
  key: id
  checks:
    - unique: id
    - not_null: id
*/
-- Current orders, folded from the changelog in streams.orders. The latest
-- change per id wins; a latest `_change` of `delete` drops the row.
-- full_merge treats this query as the whole table, so a missing id is deleted
-- from the replica. A stream flush re-runs this model.
WITH latest AS (
    SELECT
        id::BIGINT AS id,
        customer,
        CAST(amount AS DECIMAL(10, 2)) AS amount,
        status,
        updated_at::TIMESTAMPTZ AS updated_at,
        _change,
        row_number() OVER (PARTITION BY id ORDER BY _offset DESC) AS _rn
    FROM streams.orders
    WHERE id IS NOT NULL
)
SELECT id, customer, amount, status, updated_at
FROM latest
WHERE _rn = 1
  AND _change IS DISTINCT FROM 'delete'
