-- Test that all revenue amounts are positive
-- This ensures data integrity for financial reporting

select
    order_id,
    revenue_usd
from {{ ref('fact_service_operations') }}
where revenue_usd <= 0

