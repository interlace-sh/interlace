/*
interlace:
  materialise: view
  checks:
    - not_null: order_id
    - unique: order_id
    - expression: {expression: order_total - tax_paid = subtotal}
*/
with

source as (

    select * from raw_orders

),

renamed as (

    select

        ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        ---------- numerics
        subtotal as subtotal_cents,
        tax_paid as tax_paid_cents,
        order_total as order_total_cents,
        (subtotal / 100)::numeric(16, 2) as subtotal,
        (tax_paid / 100)::numeric(16, 2) as tax_paid,
        (order_total / 100)::numeric(16, 2) as order_total,

        ---------- timestamps
        date_trunc('day', ordered_at) as ordered_at

    from source

)

select * from renamed
