-- Test that service dates are not in the future
-- and completion dates are after service dates

select
    order_id,
    service_date,
    completion_date
from {{ ref('fact_service_operations') }}
where service_date > current_date()
   or (completion_date is not null and completion_date < service_date)

