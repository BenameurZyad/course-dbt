with
    source as (
        select
            -- UUID for each unique order on platform
            order_id,
            -- UserId of the user that placed this order
            user_id,
            -- PromoId if any was used in the order
            promo_id,
            -- Delivery address for this order
            address_id,
            -- Timestamp when the order was created
            created_at,
            -- date when the order was created
            created_at::date as created_date,
            -- Dollar about of the items in the order
            order_cost,
            -- Cost of shipping for the order
            shipping_cost,
            -- Total cost of the order including shipping
            order_total,
            -- Tracking number for the order/package
            tracking_id,
            -- Company that was used for shipping
            shipping_service,
            -- Estimated timestamp of delivery
            estimated_delivery_at,
            -- Estimated date of delivery
            estimated_delivery_at::date as estimated_delivery_date,
            -- Actual timestamp of delivery
            delivered_at,
            -- Actual date of delivery
            delivered_at::date as delivered_date,
            -- Status of the order
            status

        from {{ source("postgres", "orders") }}
    )

select *
from source
