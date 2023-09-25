with
    orders as (select * from {{ ref("stg__orders") }}),

    orders_items as (select * from {{ ref("stg__order_items") }}),

    products as (select * from {{ ref("stg__products") }}),

    promos as (select * from {{ ref("stg__promos") }}),

    orders_details as (

        select
            orders.user_id,
            orders.order_id,
            products.product_id,
            products.product_name,
            products.price as cost,
            orders_items.quantity as quantity,
            cost * quantity as product_cost,
            orders.promo_id,
            case
                when promos.discount is null then 0 else promos.discount
            end as applied_discount,
            product_cost - applied_discount as final_cost,
            orders.created_at,
            orders.created_date

        from orders
        left join orders_items using (order_id)
        left join products using (product_id)

        left join promos using (promo_id)
    )

select *
from orders_details
