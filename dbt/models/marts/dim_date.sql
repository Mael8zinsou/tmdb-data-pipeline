{#
    Dimension calendaire générée par dbt_utils.date_spine.
    Plage configurable via vars dans dbt_project.yml.
#}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ var('date_range_start') ~ "')",
        end_date="to_date('" ~ var('date_range_end') ~ "')"
    ) }}
),

renamed as (
    select date_day from date_spine
)

select
    cast(replace(to_varchar(date_day, 'YYYY-MM-DD'), '-', '') as number) as date_id,
    date_day,
    year(date_day)        as year,
    quarter(date_day)     as quarter,
    month(date_day)       as month,
    monthname(date_day)   as month_name,
    day(date_day)         as day_of_month,
    dayofweek(date_day)   as day_of_week,
    dayname(date_day)     as day_name,
    weekofyear(date_day)  as week_of_year,
    case
        when dayofweek(date_day) in (0, 6) then true
        else false
    end as is_weekend,
    floor(year(date_day) / 10) * 10 as decade
from renamed
