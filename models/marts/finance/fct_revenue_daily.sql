{{
    config(
        materialized='incremental',
        unique_key=['date_day', 'customer_id', 'service_type'],
        on_schema_change='append_new_columns',
        tags=['marts', 'finance', 'incremental']
    )
}}

with daily_revenue as (
    select * from {{ ref('int_service_revenue_daily') }}
    {% if is_incremental() %}
        where date_day >= (select max(date_day) from {{ this }})
    {% endif %}
),

customers as (
    select customer_id, customer_name, contract_tier from {{ ref('dim_customers') }}
),

final as (
    select
        -- Keys
        dr.date_day,
        dr.customer_id,
        dr.service_type,
        
        -- Denormalized attributes
        c.customer_name,
        c.contract_tier,
        
        -- Metrics
        dr.order_count,
        dr.total_revenue_usd,
        dr.avg_revenue_per_order_usd,
        dr.total_service_days,
        dr.total_crew_deployed,
        
        -- Calculated metrics
        dr.total_revenue_usd / nullif(dr.total_service_days, 0) as revenue_per_service_day_usd,
        dr.total_revenue_usd / nullif(dr.total_crew_deployed, 0) as revenue_per_crew_member_usd,
        
        -- Date components for easy aggregation
        extract(year from dr.date_day) as year,
        extract(month from dr.date_day) as month,
        extract(quarter from dr.date_day) as quarter,
        
        -- Metadata
        current_timestamp() as _loaded_at
        
    from daily_revenue dr
    left join customers c on dr.customer_id = c.customer_id
)

select * from final

