{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimensions']
    )
}}

with well_performance as (
    select * from {{ ref('int_well_performance') }}
),

final as (
    select
        -- Primary key
        well_id,
        
        -- Foreign keys
        basin_id,
        basin_name,
        region,
        customer_id,
        
        -- Attributes
        well_name,
        well_type,
        status,
        
        -- Drilling metrics
        total_depth_ft,
        lateral_length_ft,
        drilling_duration_days,
        
        -- Production metrics
        daily_production_boe,
        estimated_reserves_boe,
        production_per_1000ft_lateral,
        reserves_per_1000ft_lateral,
        
        -- Dates
        spud_date,
        completion_date,
        days_since_completion,
        
        -- Categorize well maturity
        case 
            when days_since_completion <= 90 then 'New (0-3 months)'
            when days_since_completion <= 365 then 'Maturing (3-12 months)'
            when days_since_completion <= 730 then 'Mature (1-2 years)'
            else 'Legacy (2+ years)'
        end as well_maturity_category,
        
        -- Metadata
        current_timestamp() as _updated_at
        
    from well_performance
)

select * from final

