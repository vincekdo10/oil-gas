{{
    config(
        materialized='view',
        tags=['staging', 'safety']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_safety_incidents') }}
),

renamed as (
    select
        -- Primary key
        incident_id,
        
        -- Foreign keys
        rig_id,
        
        -- Incident attributes
        incident_type,
        severity,
        status,
        description,
        corrective_action,
        
        -- Impact metrics
        days_away_from_work,
        restricted_work_days,
        
        -- Dates
        incident_date::date as incident_date,
        case 
            when nullif(trim(resolution_date), '') is not null 
            then resolution_date::date 
            else null 
        end as resolution_date,
        
        -- Metadata
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed

