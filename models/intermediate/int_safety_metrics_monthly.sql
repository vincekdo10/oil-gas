{{
    config(
        materialized='view',
        tags=['intermediate', 'safety']
    )
}}

with incidents as (
    select * from {{ ref('stg_safety_incidents') }}
),

rigs as (
    select * from {{ ref('stg_rigs') }}
),

monthly_metrics as (
    select
        date_trunc('month', i.incident_date) as incident_month,
        i.rig_id,
        r.rig_name,
        r.basin,
        r.rig_type,
        
        -- Incident counts by severity
        count(distinct i.incident_id) as total_incidents,
        count(distinct case when i.severity = 'Low' then i.incident_id end) as low_severity_count,
        count(distinct case when i.severity = 'Medium' then i.incident_id end) as medium_severity_count,
        count(distinct case when i.severity = 'High' then i.incident_id end) as high_severity_count,
        count(distinct case when i.severity = 'Critical' then i.incident_id end) as critical_severity_count,
        
        -- Impact metrics
        sum(i.days_away_from_work) as total_days_away_from_work,
        sum(i.restricted_work_days) as total_restricted_work_days,
        
        -- Resolution metrics
        count(distinct case when i.status = 'Resolved' then i.incident_id end) as resolved_count,
        count(distinct case when i.status = 'Under Investigation' then i.incident_id end) as under_investigation_count,
        count(distinct case when i.status = 'Open' then i.incident_id end) as open_count,
        
        -- Calculate average resolution time
        avg(
            case 
                when i.resolution_date is not null 
                then datediff(day, i.incident_date, i.resolution_date)
                else null
            end
        ) as avg_resolution_days
        
    from incidents i
    left join rigs r on i.rig_id = r.rig_id
    group by 1, 2, 3, 4, 5
)

select * from monthly_metrics

