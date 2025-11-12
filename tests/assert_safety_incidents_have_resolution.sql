-- Test that resolved safety incidents have resolution dates

select
    incident_id,
    status,
    incident_date,
    resolution_date
from {{ ref('stg_safety_incidents') }}
where status = 'Resolved'
  and resolution_date is null

