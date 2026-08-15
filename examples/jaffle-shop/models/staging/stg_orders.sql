/*
interlace:
  name: stg_orders
  checks:
    - unique: order_id
    - not_null: order_id
    - accepted_values:
        column: status
        values: [placed, shipped, completed, return_pending, returned]
*/
with source as (

    select * from raw_orders

),

renamed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from source

)

select * from renamed
