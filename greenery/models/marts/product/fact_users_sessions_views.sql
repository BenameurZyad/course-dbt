with
    -- refs
    session_event_count as (select * from {{ ref("int_session_event_count_agg") }}),

    -- CTEs
    final as (

        select 
            user_id,
            session_id,
            created_at,
            sum(page_view) as page_view_count,
            sum(add_to_cart) as add_to_cart_count,
            sum(checkout) as checkout_count,
            sum(package_shipped) as shipping_count

        from session_event_count
        group by all
    )

select *
from final
