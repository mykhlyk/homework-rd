-- gold.agg_zone_hourly — ЕТАП 2. Grain: (requested_hour, pickup_zone_key). SPEC.md, розділ 4.4.
-- Перераховуємо ЦІЛІ години, яких торкнулися нові поїздки: unique_key='requested_hour' +
-- delete+insert викидає всі бакети цих годин (зокрема спорожнілі старі зони) і вставляє свіжі.
{{
    config(
        materialized='incremental',
        unique_key='requested_hour',
        incremental_strategy='delete+insert',
    )
}}

with f as (
    select *
    from {{ ref('fact_ride') }}
    {% if is_incremental() %}
    where requested_hour in (
        select distinct requested_hour
        from {{ ref('fact_ride') }}
        where _ingested_at > {{ high_watermark() }}
    )
    {% endif %}
)

select
    requested_hour,
    pickup_zone_key,
    count(*)                                                                            as rides_requested,
    count(*) filter (where status = 'completed')                                       as rides_completed,
    count(*) filter (where status = 'cancelled')                                       as rides_cancelled,
    coalesce(sum(total_amount) filter (where status = 'completed'), 0)::numeric(12, 2) as gross_revenue,
    coalesce(sum(tip_amount)   filter (where status = 'completed'), 0)::numeric(12, 2) as tips,
    avg(wait_seconds)::numeric(10, 2)                                                   as avg_wait_seconds,
    max(_ingested_at)                                                                   as _ingested_at,
    '{{ run_started_at }}'::timestamptz                                                 as _loaded_at
from f
group by requested_hour, pickup_zone_key
