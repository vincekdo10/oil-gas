{{
    config(
        materialized='view',
        tags=['intermediate', 'operations']
    )
}}

with rigs as (
    select * from {{ ref('stg_rigs') }}
),

date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2022-01-01' as date)",
        end_date="cast('2024-12-31' as date)"
    ) }}
),

rig_days as (
    select
        r.rig_id,
        r.rig_name,
        r.basin,
        r.rig_type,
        d.date_day,
        r.day_rate_usd,
        r.status,
        
        -- Calculate utilization
        case 
            when r.status = 'Active' then 1
            when r.status = 'Idle' then 0
            when r.status = 'Maintenance' then 0
            else 0
        end as is_active,
        
        -- Calculate potential revenue
        case 
            when r.status = 'Active' then r.day_rate_usd
            else 0
        end as daily_revenue_potential
        
    from date_spine d
    cross join rigs r
    where d.date_day >= r.commissioning_date
      and d.date_day <= current_date()
)

select * from rig_days

