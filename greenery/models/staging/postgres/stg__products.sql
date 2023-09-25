with
    source as (
        select
            -- UUID for each unique product on platform
            product_id,
            -- Name of the product
            name as product_name,
            -- Price of the product
            price,
            -- Amount of the inventory we have for this product
            inventory

        from {{ source("postgres", "products") }}
    )

select *
from source
