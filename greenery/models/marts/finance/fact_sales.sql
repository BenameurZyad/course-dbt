with
    -- refs
    orders as (select * from {{ ref("stg__orders") }}),
    
    order_details as (select * from {{ ref("int__orders_details") }}),

    -- CTEs
    orders_aggregated_info as (

        select
            order_id,
            sum(product_cost) as order_cost_no_shipping_no_discount,
            sum(applied_discount) as order_discount,
            sum(final_cost) as order_final_cost


        from order_details
        group by order_id

    ),

    final as(

        select 
            orders.order_id,
            orders.created_at,
            orders.created_date,
            orders_aggregated_info.order_cost_no_shipping_no_discount,
            orders.shipping_cost,
            orders.order_total as order_cost_with_shipping_no_discount,
            case 
                when orders.promo_id is not null
                then 1
                else 0
            end as has_discount,
            orders.promo_id,
            orders_aggregated_info.order_discount,
            orders_aggregated_info.order_final_cost as order_cost_with_discount,
            orders_aggregated_info.order_final_cost 
            + orders.shipping_cost as order_cost_with_shipping_and_discount

        from orders
        left join orders_aggregated_info using (order_id)

    )

select * from final
