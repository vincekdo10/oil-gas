{{
    config(
        materialized='table',
        tags=['marts', 'finance']
    )
}}

with service_orders as (
    select * from {{ ref('fact_service_operations') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

customer_summary as (
    select
        c.customer_id,
        c.customer_name,
        c.customer_type,
        c.contract_tier,
        c.headquarters_city,
        c.headquarters_state,
        c.years_as_customer,
        
        -- Order metrics
        count(distinct so.order_id) as total_orders,
        count(distinct case when so.is_completed then so.order_id end) as completed_orders,
        count(distinct case when not so.is_completed then so.order_id end) as active_orders,
        
        -- Revenue metrics (all time)
        sum(so.revenue_usd) as total_revenue_usd,
        avg(so.revenue_usd) as avg_revenue_per_order_usd,
        
        -- Revenue by service type
        sum(case when so.service_type = 'Drilling' then so.revenue_usd else 0 end) as drilling_revenue_usd,
        sum(case when so.service_type = 'Completion' then so.revenue_usd else 0 end) as completion_revenue_usd,
        sum(case when so.service_type = 'Stimulation' then so.revenue_usd else 0 end) as stimulation_revenue_usd,
        
        -- Time metrics
        min(so.service_date) as first_service_date,
        max(so.service_date) as latest_service_date,
        datediff(day, min(so.service_date), max(so.service_date)) as customer_lifetime_days,
        
        -- Last 12 months metrics
        sum(case when so.service_date >= dateadd(month, -12, current_date()) then so.revenue_usd else 0 end) as revenue_ltm_usd,
        count(distinct case when so.service_date >= dateadd(month, -12, current_date()) then so.order_id end) as orders_ltm,
        
        -- Metadata
        current_timestamp() as _updated_at
        
    from customers c
    left join service_orders so on c.customer_id = so.customer_id
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from customer_summary
order by total_revenue_usd desc

