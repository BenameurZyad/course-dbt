with
    -- refs
    orders as (select * from {{ ref("stg__orders") }}),

    fact_shipment as (
        
        select
            order_id,
            user_id,
            tracking_id,
            status,
            shipping_service,
            estimated_delivery_at,
            estimated_delivery_date,
            delivered_at,
            delivered_date,
            DATEDIFF(day, created_at, delivered_at) as order_processing_time_in_days,
            DATEDIFF(day, estimated_delivery_at, delivered_at) as delivery_variance_in_days,
            DATEDIFF(hour, estimated_delivery_at, delivered_at) as delivery_variance_in_hours,
            case
                when delivery_variance_in_days < 0 
                then 'Early'
                when delivery_variance_in_days = 0
                then 'On Time'
                when delivery_variance_in_days > 0 and delivery_variance_in_days <= 2 
                then 'Late'
                when delivery_variance_in_days > 2
                then 'Very Late'
            end as shipping_performance

        from orders
    )

    select * from fact_shipment
    