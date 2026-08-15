/*
interlace:
  name: stg_payments
  checks:
    - unique: payment_id
    - not_null: payment_id
    - accepted_values:
        column: payment_method
        values: [credit_card, coupon, bank_transfer, gift_card]
*/
with source as (

    select * from raw_payments

),

renamed as (

    select
        id as payment_id,
        order_id,
        payment_method,

        -- `amount` is currently stored in cents, so we convert it to dollars
        amount / 100 as amount

    from source

)

select * from renamed
