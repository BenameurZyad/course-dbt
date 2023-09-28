with
    -- refs
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

        select distinct
            order_details.order_id,
            order_details.user_id,
            orders_aggregated_info.order_unique_items,
            orders_aggregated_info.order_total_items,
            order_details.created_date,
            order_details.created_at

        from order_details
        left join orders_aggregated_info using (order_id)

    )

select * from final
