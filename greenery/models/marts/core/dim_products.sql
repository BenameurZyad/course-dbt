with
    events as (select distinct product_id, page_url from {{ ref("stg__events") }}),

    products as (select * from {{ ref("stg__products") }}),

    final as (
        select 
            products.* exclude inventory, 
            events.page_url

        from products
        left join events using (product_id)
    )

select *
from final
