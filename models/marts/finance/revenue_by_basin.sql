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

revenue_by_basin as (
    select
        coalesce(well_basin, rig_basin, 'Unknown') as basin,
        
        -- Counts
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        count(distinct rig_id) as unique_rigs,
        count(distinct well_id) as unique_wells,
        
        -- Revenue metrics
        sum(revenue_usd) as total_revenue_usd,
        avg(revenue_usd) as avg_revenue_per_order_usd,
        
        -- Service type breakdown
        count(distinct case when service_type = 'Drilling' then order_id end) as drilling_orders,
        count(distinct case when service_type = 'Completion' then order_id end) as completion_orders,
        count(distinct case when service_type = 'Stimulation' then order_id end) as stimulation_orders,
        
        sum(case when service_type = 'Drilling' then revenue_usd else 0 end) as drilling_revenue_usd,
        sum(case when service_type = 'Completion' then revenue_usd else 0 end) as completion_revenue_usd,
        sum(case when service_type = 'Stimulation' then revenue_usd else 0 end) as stimulation_revenue_usd,
        
        -- Operational metrics
        sum(duration_days) as total_service_days,
        avg(duration_days) as avg_duration_days,
        
        -- Time metrics
        min(service_date) as first_service_date,
        max(service_date) as latest_service_date,
        
        -- Metadata
        current_timestamp() as _updated_at
        
    from service_orders
    group by 1
)

select * from revenue_by_basin
order by total_revenue_usd desc

