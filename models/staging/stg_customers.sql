{{
    config(
        materialized='view',
        tags=['staging', 'customers']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_customers') }}
),

renamed as (
    select
        -- Primary key
        customer_id,
        
        -- Customer attributes
        customer_name,
        customer_type,
        contract_tier,
        
        -- Location
        headquarters_city,
        headquarters_state,
        
        -- Metrics
        annual_revenue_millions * 1000000 as annual_revenue_usd,
        employee_count,
        years_as_customer,
        
        -- Contact
        primary_contact_email,
        
        -- Metadata
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed

