with
    -- refs
    page_views_count as (select * from {{ ref("int_session_event_count_agg") }}),

    -- CTEs
    final as (

        select distinct
            product_id,
            sum(page_view) as page_view_count,
            sum(add_to_cart) as add_to_cart_count,
            sum(checkout) as checkout_count,
            sum(package_shipped) as shipping_count

        from page_views_count
        group by all
    )

select *
from final
