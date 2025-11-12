{{
    config(
        materialized='view',
        tags=['staging', 'wells']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_wells') }}
),

renamed as (
    select
        -- Primary key
        well_id,
        
        -- Foreign keys
        basin_id,
        customer_id,
        
        -- Well attributes
        well_name,
        well_type,
        status,
        operator_name,
        
        -- Measurements
        total_depth_ft,
        lateral_length_ft,
        daily_production_boe,
        estimated_reserves_boe,
        
        -- Dates
        spud_date::date as spud_date,
        completion_date::date as completion_date,
        
        -- Metadata
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed

