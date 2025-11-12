{{
    config(
        materialized='table',
        tags=['marts', 'operations']
    )
}}

with equipment as (
    select * from {{ ref('dim_equipment') }}
),

maintenance_schedule as (
    select
        equipment_id,
        equipment_name,
        equipment_type,
        assigned_rig_id,
        rig_name,
        basin,
        status,
        
        -- Maintenance info
        last_maintenance_date,
        days_since_maintenance,
        maintenance_due_flag,
        
        -- Equipment age
        equipment_age_years,
        remaining_life_years,
        equipment_age_category,
        
        -- Calculate next maintenance date (assume 180 day cycle)
        dateadd(day, 180, last_maintenance_date) as next_scheduled_maintenance_date,
        
        -- Days until next maintenance
        datediff(day, current_date(), dateadd(day, 180, last_maintenance_date)) as days_until_next_maintenance,
        
        -- Priority scoring
        case 
            when days_since_maintenance > 270 then 'Critical - Overdue'
            when days_since_maintenance > 180 then 'High - Due Now'
            when days_since_maintenance > 150 then 'Medium - Due Soon'
            else 'Low - On Schedule'
        end as maintenance_priority,
        
        -- Financial
        annual_maintenance_cost_usd,
        current_book_value_usd,
        
        -- Metadata
        current_timestamp() as _updated_at
        
    from equipment
)

select * from maintenance_schedule
order by 
    case maintenance_priority
        when 'Critical - Overdue' then 1
        when 'High - Due Now' then 2
        when 'Medium - Due Soon' then 3
        else 4
    end,
    days_since_maintenance desc

