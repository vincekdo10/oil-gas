{% snapshot snap_customer_contracts %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='_loaded_at',
        tags=['snapshots', 'scd']
    )
}}

select
    customer_id,
    customer_name,
    contract_tier,
    annual_revenue_usd,
    years_as_customer,
    primary_contact_email,
    _loaded_at
from {{ ref('stg_customers') }}

{% endsnapshot %}

