{% snapshot snap_rig_rates %}

{{
    config(
        target_schema='snapshots',
        unique_key='rig_id',
        strategy='timestamp',
        updated_at='_loaded_at',
        tags=['snapshots', 'scd']
    )
}}

select
    rig_id,
    rig_name,
    basin,
    rig_type,
    day_rate_usd as current_day_rate_usd,
    status,
    _loaded_at
from {{ ref('stg_rigs') }}

{% endsnapshot %}

