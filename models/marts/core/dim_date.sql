{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimensions']
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
),

final as (
    select
        date_day,
        
        -- Date components
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(quarter from date_day) as quarter,
        extract(dayofweek from date_day) as day_of_week,
        extract(dayofyear from date_day) as day_of_year,
        extract(week from date_day) as week_of_year,
        
        -- Formatted dates
        to_char(date_day, 'YYYY-MM') as year_month,
        to_char(date_day, 'YYYY-Qx') as year_quarter,
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Day') as day_name,
        
        -- Date flags
        case when extract(dayofweek from date_day) in (0, 6) then true else false end as is_weekend,
        case when extract(month from date_day) = extract(month from current_date()) 
             and extract(year from date_day) = extract(year from current_date()) 
             then true else false end as is_current_month,
        case when extract(year from date_day) = extract(year from current_date()) 
             then true else false end as is_current_year,
        
        -- Relative periods
        datediff(day, date_day, current_date()) as days_from_today,
        datediff(week, date_day, current_date()) as weeks_from_today,
        datediff(month, date_day, current_date()) as months_from_today,
        
        -- First/last of period
        date_trunc('month', date_day) as first_day_of_month,
        last_day(date_day) as last_day_of_month,
        date_trunc('quarter', date_day) as first_day_of_quarter,
        date_trunc('year', date_day) as first_day_of_year
        
    from date_spine
)

select * from final

