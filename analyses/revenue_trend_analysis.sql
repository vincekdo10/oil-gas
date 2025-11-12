-- Revenue Trend Analysis
-- Analyze revenue trends by service type, basin, and customer tier
-- Use this for executive reporting and forecasting

with monthly_revenue as (
    select
        date_trunc('month', service_date) as revenue_month,
        service_type,
        coalesce(well_basin, rig_basin) as basin,
        contract_tier,
        sum(revenue_usd) as total_revenue_usd,
        count(distinct order_id) as order_count,
        count(distinct customer_id) as unique_customers
    from {{ ref('fact_service_operations') }}
    where is_completed = true
    group by 1, 2, 3, 4
),

revenue_with_growth as (
    select
        revenue_month,
        service_type,
        basin,
        contract_tier,
        total_revenue_usd,
        order_count,
        unique_customers,
        
        -- Calculate month-over-month growth
        lag(total_revenue_usd, 1) over (
            partition by service_type, basin, contract_tier 
            order by revenue_month
        ) as prev_month_revenue_usd,
        
        -- Calculate YoY growth
        lag(total_revenue_usd, 12) over (
            partition by service_type, basin, contract_tier 
            order by revenue_month
        ) as yoy_revenue_usd,
        
        -- Calculate rolling 3-month average
        avg(total_revenue_usd) over (
            partition by service_type, basin, contract_tier 
            order by revenue_month 
            rows between 2 preceding and current row
        ) as rolling_3mo_avg_revenue_usd
        
    from monthly_revenue
)

select
    revenue_month,
    service_type,
    basin,
    contract_tier,
    total_revenue_usd,
    order_count,
    unique_customers,
    prev_month_revenue_usd,
    yoy_revenue_usd,
    rolling_3mo_avg_revenue_usd,
    
    -- Calculate growth rates
    case 
        when prev_month_revenue_usd > 0 
        then ((total_revenue_usd - prev_month_revenue_usd) / prev_month_revenue_usd) * 100
        else null
    end as mom_growth_pct,
    
    case 
        when yoy_revenue_usd > 0 
        then ((total_revenue_usd - yoy_revenue_usd) / yoy_revenue_usd) * 100
        else null
    end as yoy_growth_pct
    
from revenue_with_growth
order by revenue_month desc, total_revenue_usd desc

