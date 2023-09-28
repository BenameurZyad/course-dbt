with
    session_views as (
        select distinct
            * exclude user_id
        from {{ ref("fact_users_sessions_views") }}),

    final as (
        select 
            round( sum(checkout_count) / sum(page_view_count) * 100, 2) as conversion_rate
        from session_views
    )

select * from final
