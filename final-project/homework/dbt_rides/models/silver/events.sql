-- silver.events — ЕТАП 2. Grain: одна подія (event_id). SPEC.md, розділ 4.2.
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert',
    )
}}

with src as (
    select *
    from {{ source('bronze', 'raw_events') }}
    where source is distinct from 'loadtest'
      and occurred_at is not null
      and ride_id is not null
    {% if is_incremental() %}
      and _ingested_at > {{ high_watermark() }}
    {% endif %}
),

deduped as (
    select
        *,
        row_number() over (
            partition by event_id
            order by _ingested_at asc, _source_file asc
        ) as _rn
    from src
)

select
    event_id,
    event_type,
    ride_id,
    source,
    occurred_at,
    {{ utc_date('occurred_at') }}        as occurred_date,
    payload::jsonb                       as payload,
    _source_file,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz  as _loaded_at
from deduped
where _rn = 1
