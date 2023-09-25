with
    source as (
        select
            -- UUID for each unique user on platform
            user_id,
            -- first name of the user
            first_name,
            -- last name of the user
            last_name,
            -- email address of the user
            email,
            -- phone number of the user
            phone_number,
            -- default delivery address for the user
            address_id,
            -- timestamp the user was created
            created_at,
            -- date the user was created
            created_at::date as created_date,
            -- timestamp the user was last updated
            updated_at,
            -- date the user was last updated
            updated_at::date as updated_date

        from {{ source("postgres", "users") }}
    )

select *
from source
