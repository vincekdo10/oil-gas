{{
    config(
        materialized='table',
        tags=['marts', 'operations']
    )
}}

with rig_utilization as (
    select
        rig_id,
        rig_name,
        basin,
        rig_type,
        
        -- Last 30 days
        sum(case when date_day >= dateadd(day, -30, current_date()) and is_active = 1 then 1 else 0 end) as active_days_last_30,
        sum(case when date_day >= dateadd(day, -30, current_date()) then 1 else 0 end) as total_days_last_30,
        
        -- Last 90 days
        sum(case when date_day >= dateadd(day, -90, current_date()) and is_active = 1 then 1 else 0 end) as active_days_last_90,
        sum(case when date_day >= dateadd(day, -90, current_date()) then 1 else 0 end) as total_days_last_90,
        
        -- Last 12 months
        sum(case when date_day >= dateadd(month, -12, current_date()) and is_active = 1 then 1 else 0 end) as active_days_ltm,
        sum(case when date_day >= dateadd(month, -12, current_date()) then 1 else 0 end) as total_days_ltm,
        
        -- Revenue
        sum(case when date_day >= dateadd(month, -12, current_date()) then daily_revenue_potential else 0 end) as revenue_potential_ltm_usd
        
    from {{ ref('int_rig_utilization_daily') }}
    group by 1, 2, 3, 4
),

rigs as (
    select * from {{ ref('dim_rigs') }}
),

final as (
    select
        r.rig_id,
        r.rig_name,
        r.basin,
        r.rig_type,
        r.status,
        r.current_day_rate_usd,
        r.rig_age_years,
        
        -- Utilization percentages
        case when ru.total_days_last_30 > 0 
             then (ru.active_days_last_30::float / ru.total_days_last_30::float) * 100 
             else 0 end as utilization_pct_last_30,
             
        case when ru.total_days_last_90 > 0 
             then (ru.active_days_last_90::float / ru.total_days_last_90::float) * 100 
             else 0 end as utilization_pct_last_90,
             
        case when ru.total_days_ltm > 0 
             then (ru.active_days_ltm::float / ru.total_days_ltm::float) * 100 
             else 0 end as utilization_pct_ltm,
        
        -- Days
        ru.active_days_last_30,
        ru.active_days_last_90,
        ru.active_days_ltm,
        
        -- Revenue
        ru.revenue_potential_ltm_usd,
        
        -- Categorize utilization
        case 
            when ru.total_days_ltm = 0 then 'Insufficient Data'
            when (ru.active_days_ltm::float / ru.total_days_ltm::float) >= 0.85 then 'High (85%+)'
            when (ru.active_days_ltm::float / ru.total_days_ltm::float) >= 0.70 then 'Good (70-85%)'
            when (ru.active_days_ltm::float / ru.total_days_ltm::float) >= 0.50 then 'Fair (50-70%)'
            else 'Low (<50%)'
        end as utilization_category_ltm,
        
        -- Metadata
        current_timestamp() as _updated_at
        
    from rigs r
    left join rig_utilization ru on r.rig_id = ru.rig_id
)

select * from final
order by utilization_pct_ltm desc

