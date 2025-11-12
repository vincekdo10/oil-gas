{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimensions']
    )
}}

with basins as (
    select * from {{ ref('stg_basins') }}
),

final as (
    select
        -- Primary key
        basin_id,
        
        -- Attributes
        basin_name,
        region,
        state,
        basin_type,
        
        -- Measurements
        total_area_sq_miles,
        active_wells,
        proven_reserves_boe,
        
        -- Metadata
        discovery_year,
        primary_formation,
        
        -- Calculated fields
        case 
            when active_wells > 0 
            then proven_reserves_boe / active_wells
            else 0
        end as avg_reserves_per_well_boe,
        
        -- Basin categorization
        case 
            when basin_type = 'Oil' then 'Oil-Focused'
            when basin_type = 'Gas' then 'Gas-Focused'
            when basin_type = 'Oil & Gas' then 'Mixed'
            else 'Other'
        end as basin_category,
        
        -- Timestamp
        current_timestamp() as _updated_at
        
    from basins
)

select * from final

