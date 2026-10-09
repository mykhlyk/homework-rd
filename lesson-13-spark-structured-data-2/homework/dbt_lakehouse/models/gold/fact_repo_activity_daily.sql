{{ config(pre_hook="set spark.sql.session.timeZone = Europe/Kyiv") }}
-- Крок 10: gold.fact_repo_activity_daily. Специфікація: ../../SPEC.md → «Крок 10».
-- Грануляція: (repo_id, date_id). Багатоджерельний rollup з {{ ref('commits') }},
-- {{ ref('pull_requests') }}, {{ ref('issues') }} та {{ ref('events') }} (WatchEvent/ForkEvent).
-- Патерн: денний агрегат на джерело (метрика + нулі для решти) → union all → group by.
-- Відсутні метрики → 0, не NULL. Порядок і типи колонок у всіх CTE мають збігатися.
-- Колонки: activity_id (md5(concat_ws('|', repo_id, date_id))), repo_id, date_id, commits,
--          distinct_committers, prs_opened, prs_merged, issues_opened, issues_closed, stars, forks.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
with commits_src as (
    select
        md5(repo_name)                                  as repo_id,
        cast(date_format(pushed_at, 'yyyyMMdd') as int) as date_id,
        count(*)                                        as commits,
        count(distinct author_email)                    as distinct_committers,
        cast(0 as bigint)                               as prs_opened,
        cast(0 as bigint)                               as prs_merged,
        cast(0 as bigint)                               as issues_opened,
        cast(0 as bigint)                               as issues_closed,
        cast(0 as bigint)                               as stars,
        cast(0 as bigint)                               as forks
    from {{ ref('commits') }}
    group by md5(repo_name), cast(date_format(pushed_at, 'yyyyMMdd') as int)
),

prs_opened_src as (
    select
        md5(repo_name),
        cast(date_format(opened_at, 'yyyyMMdd') as int),
        cast(0 as bigint),
        cast(0 as bigint),
        count(*),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint)
    from {{ ref('pull_requests') }}
    where opened_at is not null
    group by md5(repo_name), cast(date_format(opened_at, 'yyyyMMdd') as int)
),

prs_merged_src as (
    select
        md5(repo_name),
        cast(date_format(merged_at, 'yyyyMMdd') as int),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        count(*),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint)
    from {{ ref('pull_requests') }}
    where merged_at is not null
    group by md5(repo_name), cast(date_format(merged_at, 'yyyyMMdd') as int)
),

issues_opened_src as (
    select
        md5(repo_name),
        cast(date_format(opened_at, 'yyyyMMdd') as int),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        count(*),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint)
    from {{ ref('issues') }}
    where opened_at is not null
    group by md5(repo_name), cast(date_format(opened_at, 'yyyyMMdd') as int)
),

issues_closed_src as (
    select
        md5(repo_name),
        cast(date_format(closed_at, 'yyyyMMdd') as int),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        count(*),
        cast(0 as bigint),
        cast(0 as bigint)
    from {{ ref('issues') }}
    where closed_at is not null
    group by md5(repo_name), cast(date_format(closed_at, 'yyyyMMdd') as int)
),

stars_src as (
    select
        md5(repo_name),
        cast(date_format(created_at, 'yyyyMMdd') as int),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        count(*),
        cast(0 as bigint)
    from {{ ref('events') }}
    where event_type = 'WatchEvent'
    group by md5(repo_name), cast(date_format(created_at, 'yyyyMMdd') as int)
),

forks_src as (
    select
        md5(repo_name),
        cast(date_format(created_at, 'yyyyMMdd') as int),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        cast(0 as bigint),
        count(*)
    from {{ ref('events') }}
    where event_type = 'ForkEvent'
    group by md5(repo_name), cast(date_format(created_at, 'yyyyMMdd') as int)
),

unioned as (
    select * from commits_src
    union all select * from prs_opened_src
    union all select * from prs_merged_src
    union all select * from issues_opened_src
    union all select * from issues_closed_src
    union all select * from stars_src
    union all select * from forks_src
)

select
    md5(concat_ws('|', repo_id, cast(date_id as string))) as activity_id,
    repo_id,
    date_id,
    sum(commits)             as commits,
    sum(distinct_committers) as distinct_committers,
    sum(prs_opened)          as prs_opened,
    sum(prs_merged)          as prs_merged,
    sum(issues_opened)       as issues_opened,
    sum(issues_closed)       as issues_closed,
    sum(stars)               as stars,
    sum(forks)               as forks
from unioned
group by repo_id, date_id

