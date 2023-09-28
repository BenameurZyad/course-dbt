with 
    events as (select * from {{ ref("stg__events") }}),

    order_details as (select * from {{ ref("int__orders_details") }})


select
    events.user_id
    ,events.session_id
    ,events.created_at
    ,events.event_type
    ,coalesce(events.product_id, order_details.product_id) as product_id
    {{ event_types('stg__events', 'event_type') }}
from events
left join order_details using (order_id)
group by all
