with    
    -- refs
    events as (select * from {{ ref("stg__events") }}),

    products as (select * from {{ ref("stg__products") }}),

    order_details as (select * from {{ ref("int__orders_details") }}),

    -- CTEs
    events_orders as (
        select
            events.user_id,
            events.event_id,
            events.session_id,
            events.page_url,
            events.created_at,
            events.created_date,
            events.event_type,
            events.order_id,
            coalesce(events.product_id, order_details.product_id) as product_id,
            order_details.promo_id,
            order_details.quantity

        from events
        left join order_details using (order_id)
    ),

    page_views_summary as (
        select
            user_id,
            session_id,
            page_url,
            event_type,
            count(distinct(event_id)) as page_view_count

        from events_orders
        where event_type = 'page_view'
        group by 1,2,3,4
    ),

    add_to_cart_summary as (
        select
            user_id,
            session_id,
            page_url,
            event_type,
            count(distinct(event_id)) as add_to_cart_count

        from events_orders
        where event_type = 'add_to_cart'
        group by 1,2,3,4
    ),

    checkout_summary as (
        select
            user_id,
            session_id,
            page_url,
            event_type,
            product_id,
            count(distinct(product_id)) as checkout_count

        from events_orders
        where event_type = 'checkout'
        group by 1,2,3,4,5
    ),

    shipping_page_summary as (
        select
            user_id,
            session_id,
            page_url,
            event_type,
            product_id,
            count(distinct(product_id)) as shipping_count

        from events_orders
        where event_type = 'package_shipped'
        group by 1,2,3,4,5
    ),

    final as (
        
        select distinct
            events_orders.user_id,
            events_orders.session_id,
            events_orders.created_date,
            events_orders.page_url,
            events_orders.event_type,
            events_orders.product_id,
            page_views_summary.* exclude (user_id, session_id, page_url, event_type),
            add_to_cart_summary.* exclude (user_id, session_id, page_url, event_type),
            checkout_summary.* exclude (user_id, session_id, page_url, event_type, product_id),
            shipping_page_summary.* exclude (user_id, session_id, page_url, event_type, product_id)

        from events_orders

        left join page_views_summary 
        on 
            page_views_summary.user_id = events_orders.user_id
            and page_views_summary.session_id = events_orders.session_id
            and page_views_summary.page_url = events_orders.page_url
            and page_views_summary.event_type = events_orders.event_type

        left join add_to_cart_summary 
        on 
            add_to_cart_summary.user_id = events_orders.user_id 
            and add_to_cart_summary.session_id = events_orders.session_id
            and add_to_cart_summary.page_url = events_orders.page_url
            and add_to_cart_summary.event_type = events_orders.event_type

        left join checkout_summary 
        on 
            checkout_summary.user_id = events_orders.user_id 
            and checkout_summary.session_id = events_orders.session_id
            and checkout_summary.page_url = events_orders.page_url
            and checkout_summary.event_type = events_orders.event_type
            and checkout_summary.product_id = events_orders.product_id

        left join shipping_page_summary 
        on 
            shipping_page_summary.user_id = events_orders.user_id 
            and shipping_page_summary.session_id = events_orders.session_id
            and shipping_page_summary.page_url = events_orders.page_url
            and shipping_page_summary.event_type = events_orders.event_type
            and shipping_page_summary.product_id = events_orders.product_id
        
    )

select *
from final
