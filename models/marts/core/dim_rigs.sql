{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimensions']
    )
}}

with rigs as (
    select * from {{ ref('stg_rigs') }}
),

rig_utilization as (
    select
        rig_id,
        count(distinct case when is_active = 1 then date_day end) as active_days_ltm,
        count(distinct date_day) as total_days_ltm,
        avg(case when is_active = 1 then day_rate_usd end) as avg_active_day_rate_ltm
    from {{ ref('int_rig_utilization_daily') }}
    where date_day >= dateadd(month, -12, current_date())
    group by 1
),

final as (
    select
        -- Primary key
        r.rig_id,
        
        -- Attributes
        r.rig_name,
        r.rig_type,
        r.basin,
        r.status,
        
        -- Specifications
        r.capacity_depth_ft,
        r.crew_size,
        r.day_rate_usd as current_day_rate_usd,
        
        -- Dates
        r.commissioning_date,
        r.last_inspection_date,
        datediff(year, r.commissioning_date, current_date()) as rig_age_years,
        datediff(day, r.last_inspection_date, current_date()) as days_since_inspection,
        
        -- Utilization metrics (last 12 months)
        coalesce(u.active_days_ltm, 0) as active_days_ltm,
        coalesce(u.total_days_ltm, 0) as total_days_ltm,
        case 
            when u.total_days_ltm > 0 
            then (u.active_days_ltm::float / u.total_days_ltm::float) * 100
            else 0
        end as utilization_pct_ltm,
        u.avg_active_day_rate_ltm,
        
        -- Metadata
        current_timestamp() as _updated_at
        
    from rigs r
    left join rig_utilization u on r.rig_id = u.rig_id
)

select * from final

