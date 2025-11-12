{{
    config(
        materialized='view',
        tags=['intermediate', 'operations']
    )
}}

with equipment as (
    select * from {{ ref('stg_equipment') }}
),

rigs as (
    select * from {{ ref('stg_rigs') }}
),

equipment_metrics as (
    select
        e.equipment_id,
        e.equipment_name,
        e.equipment_type,
        e.assigned_rig_id,
        r.rig_name,
        r.basin,
        e.status,
        
        -- Financial metrics
        e.purchase_cost_usd,
        e.annual_maintenance_cost_usd,
        
        -- Calculate age and depreciation
        datediff(year, e.purchase_date, current_date()) as equipment_age_years,
        e.expected_life_years,
        
        -- Calculate remaining useful life
        e.expected_life_years - datediff(year, e.purchase_date, current_date()) as remaining_life_years,
        
        -- Calculate depreciation percentage
        case 
            when e.expected_life_years > 0
            then (datediff(year, e.purchase_date, current_date())::float / e.expected_life_years::float) * 100
            else 100
        end as depreciation_pct,
        
        -- Calculate current book value
        case 
            when e.expected_life_years > 0
            then e.purchase_cost_usd * (1 - (datediff(year, e.purchase_date, current_date())::float / e.expected_life_years::float))
            else 0
        end as current_book_value_usd,
        
        -- Maintenance metrics
        e.last_maintenance_date,
        datediff(day, e.last_maintenance_date, current_date()) as days_since_maintenance,
        
        -- Calculate utilization
        case 
            when e.status = 'Active' then 1.0
            when e.status = 'Maintenance' then 0.5
            when e.status = 'Idle' then 0.0
            else 0.0
        end as utilization_factor,
        
        -- Dates
        e.purchase_date
        
    from equipment e
    left join rigs r on e.assigned_rig_id = r.rig_id
)

select * from equipment_metrics

