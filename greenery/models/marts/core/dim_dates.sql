{% set min_date = '2020-01-01' %}
{% set max_date = '2025-12-31' %}


-- models/dim_dates.sql
with recursive
    date_sequence as (
        select date('{{min_date}}') as date
        union all
        select dateadd(day, 1, date)
        from date_sequence
        where date < '{{max_date}}'
    )

select
    date as date_key,
    date_part(year, date) as year,
    date_part(quarter, date) as quarter,
    date_part(month, date) as month,
    date_part(day, date) as day,
    date_part(dow, date) as day_of_week,
    date_part(doy, date) as day_of_year,
    date_part(week, date) as week
from date_sequence
