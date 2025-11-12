{{
    config(
        materialized='view',
        tags=['intermediate', 'operations']
    )
}}

with wells as (
    select * from {{ ref('stg_wells') }}
),

basins as (
    select * from {{ ref('stg_basins') }}
),

well_metrics as (
    select
        w.well_id,
        w.well_name,
        w.basin_id,
        b.basin_name,
        b.region,
        w.customer_id,
        w.well_type,
        w.status,
        
        -- Drilling metrics
        w.total_depth_ft,
        w.lateral_length_ft,
        datediff(day, w.spud_date, w.completion_date) as drilling_duration_days,
        
        -- Production metrics
        w.daily_production_boe,
        w.estimated_reserves_boe,
        
        -- Calculate production efficiency
        case 
            when w.lateral_length_ft > 0 
            then w.daily_production_boe / (w.lateral_length_ft / 1000.0)
            else null
        end as production_per_1000ft_lateral,
        
        -- Calculate reserve efficiency
        case 
            when w.lateral_length_ft > 0 
            then w.estimated_reserves_boe / (w.lateral_length_ft / 1000.0)
            else null
        end as reserves_per_1000ft_lateral,
        
        -- Dates
        w.spud_date,
        w.completion_date,
        
        -- Days since completion
        datediff(day, w.completion_date, current_date()) as days_since_completion
        
    from wells w
    left join basins b on w.basin_id = b.basin_id
)

select * from well_metrics

