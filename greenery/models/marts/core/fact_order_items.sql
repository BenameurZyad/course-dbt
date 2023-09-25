with
    -- refs
    orders as (
        select distinct order_id, user_id, created_date
        from {{ ref("stg__orders") }}
    ),

    order_items as (select * from {{ ref("stg__order_items") }}),

    -- CTEs

    final as(
        select 
            orders.order_id,
            orders.user_id,
            order_items.product_id,
            order_items.quantity,
            orders.created_date

        from orders
        left join order_items using (order_id)
    )

select * from final
