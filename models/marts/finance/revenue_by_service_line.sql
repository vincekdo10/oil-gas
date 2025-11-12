{{
    config(
        materialized='table',
        tags=['marts', 'finance']
    )
}}

with service_orders as (
    select * from {{ ref('fact_service_operations') }}
    where is_completed = true
),

revenue_by_service as (
    select
        service_type,
        
        -- Counts
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        count(distinct rig_id) as unique_rigs,
        
        -- Revenue metrics
        sum(revenue_usd) as total_revenue_usd,
        avg(revenue_usd) as avg_revenue_per_order_usd,
        min(revenue_usd) as min_revenue_usd,
        max(revenue_usd) as max_revenue_usd,
        
        -- Operational metrics
        sum(duration_days) as total_service_days,
        avg(duration_days) as avg_duration_days,
        sum(crew_size) as total_crew_deployed,
        avg(crew_size) as avg_crew_size,
        
        -- Efficiency metrics
        sum(revenue_usd) / nullif(sum(duration_days), 0) as revenue_per_service_day_usd,
        sum(revenue_usd) / nullif(sum(crew_size), 0) as revenue_per_crew_member_usd,
        
        -- Time metrics
        min(service_date) as first_service_date,
        max(service_date) as latest_service_date,
        
        -- Metadata
        current_timestamp() as _updated_at
        
    from service_orders
    group by 1
)

select * from revenue_by_service
order by total_revenue_usd desc

