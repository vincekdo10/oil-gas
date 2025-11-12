{{
    config(
        materialized='view',
        tags=['staging', 'basins']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_basins') }}
),

renamed as (
    select
        -- Primary key
        basin_id,
        
        -- Basin attributes
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
        
        -- Timestamp
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed

