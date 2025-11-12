{{
    config(
        materialized='view',
        tags=['staging', 'equipment']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_equipment') }}
),

renamed as (
    select
        -- Primary key
        equipment_id,
        
        -- Foreign keys
        assigned_rig_id,
        
        -- Equipment attributes
        equipment_name,
        equipment_type,
        status,
        
        -- Financial
        purchase_cost as purchase_cost_usd,
        annual_maintenance_cost as annual_maintenance_cost_usd,
        
        -- Lifecycle
        expected_life_years,
        
        -- Dates
        purchase_date::date as purchase_date,
        last_maintenance_date::date as last_maintenance_date,
        
        -- Metadata
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed

