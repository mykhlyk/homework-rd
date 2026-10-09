-- Крок 7: gold.dim_date. Специфікація: ../../SPEC.md → «Крок 7».
-- Згенерований безперервний календар (БЕЗ seed): explode(sequence(min, max, interval 1 day)).
-- Межі min/max — підзапитом по фактичних датах з {{ ref('commits') }}, {{ ref('pull_requests') }},
-- {{ ref('issues') }} (pushed_at / opened_at / merged_at / closed_at). Не хардкодьте.
-- Колонки: date_id (int yyyyMMdd), date_day (date), day_of_week, is_weekend, iso_week, year.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
with all_dates as (
    select cast(pushed_at as date) as d from {{ ref('commits') }}
    union all
    select cast(opened_at as date) from {{ ref('pull_requests') }}
    union all
    select cast(merged_at as date) from {{ ref('pull_requests') }}
    union all
    select cast(opened_at as date) from {{ ref('issues') }}
    union all
    select cast(closed_at as date) from {{ ref('issues') }}
),

bounds as (
    select
        min(d) as min_d,
        max(d) as max_d
    from all_dates
    where d is not null
),

calendar as (
    select explode(sequence(min_d, max_d, interval 1 day)) as date_day
    from bounds
)

select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_id,
    date_day,
    dayofweek(date_day)                            as day_of_week, 
    dayofweek(date_day) in (1, 7)                  as is_weekend,
    weekofyear(date_day)                           as iso_week,
    year(date_day)                                 as year
from calendar
