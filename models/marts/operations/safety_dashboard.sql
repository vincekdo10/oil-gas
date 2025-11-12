{{
    config(
        materialized='table',
        tags=['marts', 'operations', 'safety']
    )
}}

with safety_metrics as (
    select * from {{ ref('int_safety_metrics_monthly') }}
),

rigs as (
    select rig_id, rig_name, basin, rig_type from {{ ref('dim_rigs') }}
),

-- Calculate YTD and rolling metrics
dashboard_metrics as (
    select
        sm.rig_id,
        r.rig_name,
        r.basin,
        r.rig_type,
        
        -- YTD metrics
        sum(case when extract(year from sm.incident_month) = extract(year from current_date()) then sm.total_incidents else 0 end) as incidents_ytd,
        sum(case when extract(year from sm.incident_month) = extract(year from current_date()) then sm.critical_severity_count else 0 end) as critical_incidents_ytd,
        sum(case when extract(year from sm.incident_month) = extract(year from current_date()) then sm.total_days_away_from_work else 0 end) as days_away_ytd,
        
        -- Last 12 months
        sum(case when sm.incident_month >= dateadd(month, -12, current_date()) then sm.total_incidents else 0 end) as incidents_ltm,
        sum(case when sm.incident_month >= dateadd(month, -12, current_date()) then sm.critical_severity_count else 0 end) as critical_incidents_ltm,
        
        -- Last 3 months
        sum(case when sm.incident_month >= dateadd(month, -3, current_date()) then sm.total_incidents else 0 end) as incidents_last_3m,
        
        -- Average resolution time
        avg(case when sm.incident_month >= dateadd(month, -12, current_date()) then sm.avg_resolution_days end) as avg_resolution_days_ltm,
        
        -- Last incident date
        max(sm.incident_month) as last_incident_month,
        
        -- Days since last incident
        datediff(day, max(sm.incident_month), current_date()) as days_since_last_incident,
        
        -- Metadata
        current_timestamp() as _updated_at
        
    from safety_metrics sm
    left join rigs r on sm.rig_id = r.rig_id
    group by 1, 2, 3, 4
),

-- Add safety rating
final as (
    select
        *,
        
        -- Calculate safety rating
        case 
            when incidents_ltm = 0 then 'Excellent'
            when critical_incidents_ltm = 0 and incidents_ltm <= 2 then 'Good'
            when critical_incidents_ltm <= 1 and incidents_ltm <= 5 then 'Fair'
            else 'Needs Improvement'
        end as safety_rating_ltm,
        
        -- Incident rate per 200,000 hours (assuming 24/7 operation)
        (incidents_ltm::float / (365 * 24)) * 200000 as incident_rate_ltm
        
    from dashboard_metrics
)

select * from final
order by incidents_ltm desc, critical_incidents_ltm desc

