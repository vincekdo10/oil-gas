-- Rig Utilization Forecast
-- Forecast future rig utilization based on historical trends
-- Use this for capacity planning and resource allocation

with historical_utilization as (
    select
        date_trunc('week', date_day) as week_start,
        rig_type,
        basin,
        
        -- Calculate weekly utilization
        count(distinct case when is_active = 1 then rig_id end) as active_rigs,
        count(distinct rig_id) as total_rigs,
        
        avg(case when is_active = 1 then day_rate_usd end) as avg_active_day_rate_usd,
        
        -- Calculate utilization percentage
        (count(distinct case when is_active = 1 then rig_id end)::float / 
         nullif(count(distinct rig_id), 0)::float) * 100 as utilization_pct
         
    from {{ ref('int_rig_utilization_daily') }}
    where date_day >= dateadd(month, -24, current_date())  -- Last 24 months
    group by 1, 2, 3
),

utilization_with_trends as (
    select
        week_start,
        rig_type,
        basin,
        active_rigs,
        total_rigs,
        utilization_pct,
        avg_active_day_rate_usd,
        
        -- Calculate 4-week moving average
        avg(utilization_pct) over (
            partition by rig_type, basin 
            order by week_start 
            rows between 3 preceding and current row
        ) as utilization_4wk_ma,
        
        -- Calculate 12-week moving average
        avg(utilization_pct) over (
            partition by rig_type, basin 
            order by week_start 
            rows between 11 preceding and current row
        ) as utilization_12wk_ma,
        
        -- Calculate trend (simple linear trend indicator)
        avg(utilization_pct) over (
            partition by rig_type, basin 
            order by week_start 
            rows between 11 preceding and current row
        ) - avg(utilization_pct) over (
            partition by rig_type, basin 
            order by week_start 
            rows between 23 preceding and 12 preceding
        ) as trend_indicator
        
    from historical_utilization
)

select
    week_start,
    rig_type,
    basin,
    active_rigs,
    total_rigs,
    round(utilization_pct, 2) as utilization_pct,
    round(utilization_4wk_ma, 2) as utilization_4wk_ma,
    round(utilization_12wk_ma, 2) as utilization_12wk_ma,
    round(trend_indicator, 2) as trend_indicator,
    round(avg_active_day_rate_usd, 0) as avg_active_day_rate_usd,
    
    -- Classify trend
    case 
        when trend_indicator > 5 then 'Strong Upward'
        when trend_indicator > 2 then 'Moderate Upward'
        when trend_indicator > -2 then 'Stable'
        when trend_indicator > -5 then 'Moderate Downward'
        else 'Strong Downward'
    end as trend_classification,
    
    -- Simple forecast (using 12-week MA as baseline)
    round(utilization_12wk_ma + (trend_indicator * 0.5), 2) as forecasted_utilization_4wk_ahead
    
from utilization_with_trends
where week_start >= dateadd(month, -12, current_date())  -- Show last 12 months
order by week_start desc, rig_type, basin

