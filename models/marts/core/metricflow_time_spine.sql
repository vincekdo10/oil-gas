{{
    config(
        materialized='table',
        tags=['semantic_layer']
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2025-11-13' as date)"
    ) }}
)

select
    date_day as date_day
from date_spine

