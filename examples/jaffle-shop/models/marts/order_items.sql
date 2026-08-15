/*
interlace:
  checks:
    - not_null: order_item_id
    - unique: order_item_id
    # dbt tests this against `orders`. interlace runs a model's checks when that model
    # builds, and `orders` is built FROM order_items — so the check would have to wait
    # for a model that is waiting for it. Pointed one model upstream instead: `orders`
    # is stg_orders left-joined to a summary of these rows, so the order_id set is the
    # same one, and the check is the same check.
    - relationships:
        column: order_id
        to: stg_orders
        field: order_id
*/
with

order_items as (

    select * from stg_order_items

),

orders as (

    select * from stg_orders

),

products as (

    select * from stg_products

),

supplies as (

    select * from stg_supplies

),

order_supplies_summary as (

    select
        product_id,

        sum(supply_cost) as supply_cost

    from supplies

    group by 1

),

joined as (

    select
        order_items.*,

        orders.ordered_at,

        products.product_name,
        products.product_price,
        products.is_food_item,
        products.is_drink_item,

        order_supplies_summary.supply_cost

    from order_items

    left join orders on order_items.order_id = orders.order_id

    left join products on order_items.product_id = products.product_id

    left join order_supplies_summary
        on order_items.product_id = order_supplies_summary.product_id

)

select * from joined
