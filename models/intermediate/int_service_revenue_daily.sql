{{
    config(
        materialized='view',
        tags=['intermediate', 'finance']
    )
}}

with service_orders as (
    select * from {{ ref('stg_service_orders') }}
),

date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2022-01-01' as date)",
        end_date="cast('2024-12-31' as date)"
    ) }}
),

daily_revenue as (
    select
        d.date_day,
        so.customer_id,
        so.service_type,
        
        -- Aggregate metrics
        count(distinct so.order_id) as order_count,
        sum(so.revenue_usd) as total_revenue_usd,
        avg(so.revenue_usd) as avg_revenue_per_order_usd,
        sum(so.duration_days) as total_service_days,
        sum(so.crew_size) as total_crew_deployed
        
    from date_spine d
    inner join service_orders so 
        on d.date_day >= so.service_date
        and d.date_day <= coalesce(so.completion_date, current_date())
    group by 1, 2, 3
)

select * from daily_revenue

