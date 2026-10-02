-- gold.fact_ride — ЕТАП 2. Grain: поїздка (accumulating snapshot). SPEC.md, розділ 4.4.
{{
    config(
        materialized='incremental',
        unique_key='ride_id',
        incremental_strategy='delete+insert',
    )
}}

with r as (
    select *
    from {{ ref('rides') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
)

select
    r.ride_id,
    coalesce(dz_pu.zone_key, -1)       as pickup_zone_key,
    coalesce(dz_do.zone_key, -1)       as dropoff_zone_key,
    coalesce(dd.driver_key, 'unknown') as driver_key,
    r.rider_id,
    r.status,
    r.requested_at,
    r.accepted_at,
    r.started_at,
    r.completed_at,
    r.cancelled_at,
    r.paid_at,
    {{ utc_date('r.requested_at') }}                                         as requested_date,
    date_trunc('hour', r.requested_at at time zone 'UTC') at time zone 'UTC' as requested_hour,
    r.wait_seconds,
    r.trip_seconds,
    r.distance_km,
    r.fare_amount,
    r.surge_multiplier,
    r.tolls_amount,
    r.tip_amount,
    r.total_amount,
    r.currency,
    r.payment_method,
    r.payment_amount,
    r.cancelled_by,
    r.cancel_reason,
    r.cancel_stage,
    r._ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from r
left join {{ ref('dim_zone') }}   as dz_pu on r.pickup_zone_id  = dz_pu.zone_key
left join {{ ref('dim_zone') }}   as dz_do on r.dropoff_zone_id = dz_do.zone_key
left join {{ ref('dim_driver') }} as dd    on r.driver_id       = dd.driver_key
