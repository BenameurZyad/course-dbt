with
    -- refs
    orders as (select * from {{ ref("stg__orders") }}),
    
    order_details as (select * from {{ ref("int__orders_details") }}),

    -- CTEs
    orders_aggregated_info as (

        select
            order_id,
            count(distinct(product_id)) as order_unique_items,
            sum(quantity) as order_total_items


        from order_details
        group by order_id

    ),

    final as(

        select 
            orders.order_id,
            orders.user_id,
            orders_aggregated_info.order_unique_items,
            orders_aggregated_info.order_total_items,
            orders.created_date,
            orders.created_at

        from orders
        left join orders_aggregated_info using (order_id)

    )

select * from final
