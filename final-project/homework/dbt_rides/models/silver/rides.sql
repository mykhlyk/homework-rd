-- silver.rides — ЕТАП 2. Grain: одна поїздка (ride_id). SPEC.md, розділ 4.3.
{{
    config(
        materialized='incremental',
        unique_key='ride_id',
        incremental_strategy='delete+insert',
    )
}}

with ev as (
    select *
    from {{ ref('events') }}
    {% if is_incremental() %}
    where ride_id in (
        select distinct ride_id
        from {{ ref('events') }}
        where _ingested_at > {{ high_watermark() }}
    )
    {% endif %}
),

rolled as (
    select
        ride_id,

        max(payload #>> '{rider,id}')          filter (where event_type = 'ride_requested') as rider_id,
        max(payload #>> '{rider,platform}')    filter (where event_type = 'ride_requested') as rider_platform,
        max(payload #>> '{rider,app_version}') filter (where event_type = 'ride_requested') as app_version,
        max(payload #>> '{requested_vehicle}') filter (where event_type = 'ride_requested') as requested_vehicle,

        max(payload #>> '{driver,id}') filter (
            where event_type in ('ride_accepted', 'ride_started', 'ride_completed')
        ) as driver_id,

        case
            when bool_or(event_type = 'ride_completed') then 'completed'
            when bool_or(event_type = 'ride_cancelled') then 'cancelled'
            when bool_or(event_type = 'ride_started')   then 'in_progress'
            when bool_or(event_type = 'ride_accepted')  then 'accepted'
            else 'requested'
        end as status,

        max(occurred_at) filter (where event_type = 'ride_requested')   as requested_at,
        max(occurred_at) filter (where event_type = 'ride_accepted')    as accepted_at,
        max(occurred_at) filter (where event_type = 'ride_started')     as started_at,
        max(occurred_at) filter (where event_type = 'ride_completed')   as completed_at,
        max(occurred_at) filter (where event_type = 'ride_cancelled')   as cancelled_at,
        max(occurred_at) filter (where event_type = 'payment_captured') as paid_at,

        coalesce(
            max((payload #>> '{pickup,zone_id}')::int) filter (where event_type = 'ride_started'),
            max((payload #>> '{pickup,zone_id}')::int) filter (where event_type = 'ride_requested')
        ) as pickup_zone_id,
        coalesce(
            max((payload #>> '{dropoff,zone_id}')::int) filter (where event_type = 'ride_completed'),
            max((payload #>> '{dropoff,zone_id}')::int) filter (where event_type = 'ride_requested')
        ) as dropoff_zone_id,

        max((payload #>> '{surge_estimate}')::numeric)        filter (where event_type = 'ride_requested') as surge_estimate,
        max((payload #>> '{distance_km}')::numeric)           filter (where event_type = 'ride_completed') as distance_km,
        max((payload #>> '{fare,surge_multiplier}')::numeric) filter (where event_type = 'ride_completed') as surge_multiplier,
        max((payload #>> '{fare,amount}')::numeric)           filter (where event_type = 'ride_completed') as fare_amount,
        max((payload #>> '{fare,tolls}')::numeric)            filter (where event_type = 'ride_completed') as tolls_amount,
        max((payload #>> '{fare,tip}')::numeric)              filter (where event_type = 'ride_completed') as tip_amount,
        max((payload #>> '{fare,total}')::numeric)            filter (where event_type = 'ride_completed') as total_amount,
        max(payload #>> '{fare,currency}')                    filter (where event_type = 'ride_completed') as currency,

        max(payload #>> '{cancelled_by}') filter (where event_type = 'ride_cancelled') as cancelled_by,
        max(payload #>> '{reason}')       filter (where event_type = 'ride_cancelled') as cancel_reason,
        max(payload #>> '{stage}')        filter (where event_type = 'ride_cancelled') as cancel_stage,

        max(payload #>> '{payment,method}')            filter (where event_type = 'payment_captured') as payment_method,
        max(payload #>> '{payment,psp_reference}')     filter (where event_type = 'payment_captured') as psp_reference,
        max((payload #>> '{payment,amount}')::numeric) filter (where event_type = 'payment_captured') as payment_amount,

        max(_ingested_at) as _ingested_at
    from ev
    group by ride_id
)

select
    ride_id,
    rider_id,
    rider_platform,
    app_version,
    requested_vehicle,
    driver_id,
    status,
    requested_at,
    accepted_at,
    started_at,
    completed_at,
    cancelled_at,
    paid_at,
    pickup_zone_id,
    dropoff_zone_id,
    extract(epoch from (accepted_at - requested_at))::int as wait_seconds,
    extract(epoch from (completed_at - started_at))::int  as trip_seconds,
    surge_estimate,
    distance_km::numeric(8, 2)      as distance_km,
    surge_multiplier::numeric(4, 2) as surge_multiplier,
    fare_amount::numeric(10, 2)     as fare_amount,
    tolls_amount::numeric(10, 2)    as tolls_amount,
    tip_amount::numeric(10, 2)      as tip_amount,
    total_amount::numeric(10, 2)    as total_amount,
    currency,
    cancelled_by,
    cancel_reason,
    cancel_stage,
    payment_method,
    psp_reference,
    payment_amount::numeric(10, 2)  as payment_amount,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from rolled
where requested_at is not null
