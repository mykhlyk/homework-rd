-- gold.dim_driver — ЕТАП 2. Grain: водій. SPEC.md, розділ 4.4.
{{
    config(
        materialized='incremental',
        unique_key='driver_key',
        incremental_strategy='delete+insert',
    )
}}

with accepted as (
    select
        payload #>> '{driver,id}'                as driver_key,
        (payload #>> '{driver,rating}')::numeric as rating,
        payload #>> '{driver,vehicle,type}'      as vehicle_type,
        payload #>> '{driver,vehicle,medallion}' as medallion,
        occurred_at,
        _ingested_at
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
      and payload #>> '{driver,id}' is not null
),

scoped as (
    select *
    from accepted
    {% if is_incremental() %}
    where driver_key in (
        select distinct driver_key
        from accepted
        where _ingested_at > {{ high_watermark() }}
    )
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by driver_key
            order by occurred_at desc, _ingested_at desc
        ) as _rn
    from scoped
),

spans as (
    select
        driver_key,
        min(occurred_at)  as first_seen_at,
        max(occurred_at)  as last_seen_at,
        max(_ingested_at) as _ingested_at
    from scoped
    group by driver_key
)

select
    r.driver_key,
    r.vehicle_type,
    r.medallion,
    r.rating          as latest_rating,
    s.first_seen_at,
    s.last_seen_at,
    s._ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from ranked as r
join spans as s using (driver_key)
where r._rn = 1

{% if not is_incremental() %}
union all
select
    'unknown', 'unknown', null, null, null, null,
    '-infinity'::timestamptz,
    '{{ run_started_at }}'::timestamptz
{% endif %}
