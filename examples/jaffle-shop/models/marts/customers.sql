/*
interlace:
  checks:
    - not_null: customer_id
    - unique: customer_id
    - accepted_values:
        column: customer_type
        values: [new, returning]
    - expression: {expression: lifetime_spend_pretax + lifetime_tax_paid = lifetime_spend}
*/
with

customers as (

    select * from stg_customers

),

-- `orders as (select * from orders)` in dbt: ref() renders a qualified relation,
-- so the CTE cannot shadow it. Here the reference is the model's bare name, and a
-- CTE of the same name shadows it into a circular reference. Renamed.
all_orders as (

    select * from orders

),

customer_orders_summary as (

    select
        all_orders.customer_id,

        count(distinct all_orders.order_id) as count_lifetime_orders,
        count(distinct all_orders.order_id) > 1 as is_repeat_buyer,
        min(all_orders.ordered_at) as first_ordered_at,
        max(all_orders.ordered_at) as last_ordered_at,
        sum(all_orders.subtotal) as lifetime_spend_pretax,
        sum(all_orders.tax_paid) as lifetime_tax_paid,
        sum(all_orders.order_total) as lifetime_spend

    from all_orders

    group by 1

),

joined as (

    select
        customers.*,

        customer_orders_summary.count_lifetime_orders,
        customer_orders_summary.first_ordered_at,
        customer_orders_summary.last_ordered_at,
        customer_orders_summary.lifetime_spend_pretax,
        customer_orders_summary.lifetime_tax_paid,
        customer_orders_summary.lifetime_spend,

        case
            when customer_orders_summary.is_repeat_buyer then 'returning'
            else 'new'
        end as customer_type

    from customers

    left join customer_orders_summary
        on customers.customer_id = customer_orders_summary.customer_id

)

select * from joined
