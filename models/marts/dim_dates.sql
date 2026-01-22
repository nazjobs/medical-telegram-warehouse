-- Simple date dimension generator
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2030-01-01' as date)"
    )
    }}
)

select
    date_day as date_key,
    date_day as full_date,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    to_char(date_day, 'Month') as month_name,
    extract(week from date_day) as week_of_year,
    extract(isodow from date_day) as day_of_week,
    to_char(date_day, 'Day') as day_name,
    case when extract(isodow from date_day) in (6, 7) then true else false end as is_weekend
from date_spine
