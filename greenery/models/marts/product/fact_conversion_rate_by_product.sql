with 
    dim_products as (select * from {{ ref("dim_products") }}),

    product_views as (select * from {{ ref("fact_products_views") }}),


    final as (
        select 
            dim_products.product_name,
            product_views.product_id,
            round( sum(product_views.checkout_count) / sum(product_views.page_view_count) * 100, 2) as conversion_rate
        from product_views
        left join dim_products using (product_id)
        group by all
    )

select * from final
