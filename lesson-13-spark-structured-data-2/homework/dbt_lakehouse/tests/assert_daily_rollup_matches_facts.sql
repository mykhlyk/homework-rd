-- Тест: sum(fact_repo_activity_daily.commits) = count(*) з fact_commit.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
-- TODO: замініть заглушку (зараз тест проходить вхолосту).
select
    r.rollup_commits,
    f.fact_commits
from (
    select sum(commits) as rollup_commits
    from {{ ref('fact_repo_activity_daily') }}
) r
cross join (
    select count(*) as fact_commits
    from {{ ref('fact_commit') }}
) f
where r.rollup_commits <> f.fact_commits

