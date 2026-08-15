/*
interlace:
  name: stg_customers
  checks:
    - unique: customer_id
    - not_null: customer_id
*/
with source as (

    select * from raw_customers

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name

    from source

)

select * from renamed
