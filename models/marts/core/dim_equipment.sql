{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimensions']
    )
}}

with equipment as (
    select * from {{ ref('int_equipment_utilization') }}
),

final as (
    select
        -- Primary key
        equipment_id,
        
        -- Foreign keys
        assigned_rig_id,
        
        -- Attributes
        equipment_name,
        equipment_type,
        status,
        rig_name,
        basin,
        
        -- Financial metrics
        purchase_cost_usd,
        annual_maintenance_cost_usd,
        current_book_value_usd,
        depreciation_pct,
        
        -- Lifecycle metrics
        equipment_age_years,
        expected_life_years,
        remaining_life_years,
        
        -- Maintenance
        last_maintenance_date,
        days_since_maintenance,
        
        -- Utilization
        utilization_factor,
        
        -- Categorize equipment age
        case 
            when equipment_age_years <= 2 then 'New (0-2 years)'
            when equipment_age_years <= 5 then 'Good (2-5 years)'
            when equipment_age_years <= 10 then 'Aging (5-10 years)'
            else 'Old (10+ years)'
        end as equipment_age_category,
        
        -- Flag equipment needing maintenance
        case 
            when days_since_maintenance > 180 then true
            else false
        end as maintenance_due_flag,
        
        -- Dates
        purchase_date,
        
        -- Metadata
        current_timestamp() as _updated_at
        
    from equipment
)

select * from final

