{{
    config(
        materialized='table',
        tags=['marts', 'operations']
    )
}}

with wells as (
    select * from {{ ref('dim_wells') }}
),

production_metrics as (
    select
        basin_name,
        region,
        well_type,
        well_maturity_category,
        
        -- Counts
        count(distinct well_id) as total_wells,
        count(distinct case when status = 'Producing' then well_id end) as producing_wells,
        
        -- Production metrics
        sum(daily_production_boe) as total_daily_production_boe,
        avg(daily_production_boe) as avg_daily_production_per_well_boe,
        sum(estimated_reserves_boe) as total_estimated_reserves_boe,
        
        -- Drilling metrics
        avg(total_depth_ft) as avg_total_depth_ft,
        avg(lateral_length_ft) as avg_lateral_length_ft,
        avg(drilling_duration_days) as avg_drilling_duration_days,
        
        -- Efficiency metrics
        avg(production_per_1000ft_lateral) as avg_production_per_1000ft_lateral,
        avg(reserves_per_1000ft_lateral) as avg_reserves_per_1000ft_lateral,
        
        -- Metadata
        current_timestamp() as _updated_at
        
    from wells
    group by 1, 2, 3, 4
)

select * from production_metrics
order by total_daily_production_boe desc

