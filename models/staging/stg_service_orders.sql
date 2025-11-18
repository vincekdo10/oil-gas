{{
    config(
        materialized='view',
        tags=['staging', 'service_orders']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_service_orders') }}
),

renamed as (
    select
        -- Primary key
        order_id,
        
        -- Foreign keys
        rig_id,
        well_id,
        customer_id,
        
        -- Service attributes
        service_type,
        status,
        
        -- Operations
        crew_size,
        duration_days,
        
        -- Financial
        revenue_amount as revenue_usd,
        
        -- Dates
        service_date::date as service_date,
        case 
            when nullif(trim(completion_date), '') is not null 
            then completion_date::date 
            else null 
        end as completion_date,
        
        -- Metadata
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed

