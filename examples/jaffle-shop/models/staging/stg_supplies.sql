/*
interlace:
  materialise: view
  checks:
    - not_null: supply_uuid
    - unique: supply_uuid
*/
with

source as (

    select * from raw_supplies

),

renamed as (

    select

        ----------  ids
        surrogate_key(id, sku) as supply_uuid,
        id as supply_id,
        sku as product_id,

        ---------- text
        name as supply_name,

        ---------- numerics
        cents_to_dollars(cost) as supply_cost,

        ---------- booleans
        perishable as is_perishable_supply

    from source

)

select * from renamed
