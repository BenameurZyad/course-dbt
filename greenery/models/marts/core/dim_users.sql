with
    addresses as (select * from {{ ref("stg__adresses") }}),

    users as (select * from {{ ref("stg__users") }}),

    dim_users as (

        select
            users.user_id,
            users.first_name,
            users.last_name,
            users.email,
            users.phone_number,
            users.address_id,
            addresses.address,
            addresses.zipcode,
            addresses.state,
            addresses.country,
            users.created_at,
            users.updated_at

        from users
        left join addresses using (address_id)

    )

select *
from dim_users
