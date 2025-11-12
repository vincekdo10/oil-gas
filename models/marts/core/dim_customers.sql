{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimensions']
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_revenue as (
    select
        customer_id,
        sum(revenue_usd) as total_revenue_ltm_usd,
        count(distinct order_id) as order_count_ltm
    from {{ ref('stg_service_orders') }}
    where service_date >= dateadd(month, -12, current_date())
    group by 1
),

final as (
    select
        -- Primary key
        c.customer_id,
        
        -- Attributes
        c.customer_name,
        c.customer_type,
        c.contract_tier,
        
        -- Location
        c.headquarters_city,
        c.headquarters_state,
        
        -- Company metrics
        c.annual_revenue_usd,
        c.employee_count,
        c.years_as_customer,
        
        -- Contact
        c.primary_contact_email,
        
        -- Revenue metrics (last 12 months)
        coalesce(r.total_revenue_ltm_usd, 0) as total_revenue_ltm_usd,
        coalesce(r.order_count_ltm, 0) as order_count_ltm,
        
        -- Customer tier scoring
        case 
            when c.contract_tier = 'Platinum' then 4
            when c.contract_tier = 'Gold' then 3
            when c.contract_tier = 'Silver' then 2
            when c.contract_tier = 'Bronze' then 1
            else 0
        end as tier_score,
        
        -- Metadata
        current_timestamp() as _updated_at
        
    from customers c
    left join customer_revenue r on c.customer_id = r.customer_id
)

select * from final

