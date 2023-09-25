with
    -- refs
    page_views_count as (select * from {{ ref("int_page_views_count") }}),

    -- CTEs
    final as (

        select 
            user_id,
            session_id,
            sum(page_view_count) as page_view_count,
            sum(add_to_cart_count) as add_to_cart_count,
            sum(checkout_count) as checkout_count,
            sum(shipping_count) as shipping_count

        from page_views_count
        where 
            page_view_count is not null 
            or add_to_cart_count is not null 
            or checkout_count is not null 
            or shipping_count is not null 
        group by 1,2
    )

select *
from final
