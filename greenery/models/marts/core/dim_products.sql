with
    products as (select * from {{ ref("stg__products") }}),

    final as (
        select 
            products.* exclude inventory
        from products
    )

select *
from final
