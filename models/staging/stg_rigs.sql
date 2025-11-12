{{
    config(
        materialized='view',
        tags=['staging', 'rigs']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_rigs') }}
),

renamed as (
    select
        -- Primary key
        rig_id,
        
        -- Rig attributes
        rig_name,
        rig_type,
        basin,
        status,
        
        -- Financial
        day_rate as day_rate_usd,
        
        -- Specifications
        capacity_depth_ft,
        crew_size,
        
        -- Dates
        commissioning_date::date as commissioning_date,
        last_inspection_date::date as last_inspection_date,
        
        -- Metadata
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed

