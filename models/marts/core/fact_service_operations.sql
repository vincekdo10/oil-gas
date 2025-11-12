{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='append_new_columns',
        tags=['marts', 'core', 'facts', 'incremental']
    )
}}

with service_orders as (
    select * from {{ ref('stg_service_orders') }}
    {% if is_incremental() %}
        where service_date >= (select max(service_date) from {{ this }})
    {% endif %}
),

rigs as (
    select rig_id, rig_name, rig_type, basin from {{ ref('dim_rigs') }}
),

wells as (
    select well_id, well_name, basin_id, basin_name from {{ ref('dim_wells') }}
),

customers as (
    select customer_id, customer_name, contract_tier from {{ ref('dim_customers') }}
),

final as (
    select
        -- Primary key
        so.order_id,
        
        -- Foreign keys
        so.service_date as service_date_key,
        so.rig_id,
        so.well_id,
        so.customer_id,
        
        -- Denormalized attributes for easy filtering
        r.rig_name,
        r.rig_type,
        r.basin as rig_basin,
        w.well_name,
        w.basin_name as well_basin,
        c.customer_name,
        c.contract_tier,
        
        -- Service attributes
        so.service_type,
        so.status,
        
        -- Metrics
        so.revenue_usd,
        so.crew_size,
        so.duration_days,
        so.revenue_usd / nullif(so.duration_days, 0) as avg_daily_revenue_usd,
        
        -- Dates
        so.service_date,
        so.completion_date,
        
        -- Flags
        case when so.completion_date is not null then true else false end as is_completed,
        case when so.status = 'Completed' then true else false end as is_status_completed,
        
        -- Metadata
        current_timestamp() as _loaded_at
        
    from service_orders so
    left join rigs r on so.rig_id = r.rig_id
    left join wells w on so.well_id = w.well_id
    left join customers c on so.customer_id = c.customer_id
)

select * from final

