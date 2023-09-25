with
    -- refs
    order_details as (select * from {{ ref("int__orders_details") }}),

    final as (

        select
            user_id,
            count(distinct(order_id)) as lifetime_orders,
            sum(quantity) as lifetime_ordered_items,
            count(distinct(product_name)) as distinct_ordered_items,
            case
                when lifetime_orders >= 5
                then '5 plus Orders'
                when lifetime_orders >= 3
                then '3 to 5 Orders'
                when lifetime_orders >= 2
                then '2 Orders'
                when lifetime_orders = 1
                then '1 Order'
            end as user_return_rate,
            sum(product_cost) as lifetime_value,
            sum(applied_discount) as lifetime_applied_discount,
            count(distinct(promo_id)) as lifetime_promos_used,
            sum(final_cost) as lifetime_net_value,
            min(created_date) as first_order_date,
            max(created_date) as last_order_date

        from order_details
        group by 1
    )

select *
from final
