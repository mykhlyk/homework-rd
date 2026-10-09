-- Крок 2: silver.commits. Специфікація: ../../SPEC.md → «Крок 2».
-- Джерело: {{ ref('events') }}, лише PushEvent.
-- from_json(payload, PUSH_SCHEMA) → explode масиву commits → commit grain. PUSH_SCHEMA = var('push_schema').
-- Дедуп: один рядок на commit_sha, найраніший pushed_at.
-- Колонки: commit_sha, repo_name, pushed_by, branch, author_name, author_email, message,
--          is_distinct, pushed_at, is_merge_commit, message_subject, message_length
-- Пастка: `distinct` — reserved word, у DDL-схемі та доступі до поля потрібні backticks.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
with push as (
    select
        event_id,
        repo_name,
        actor_login                                  as pushed_by,
        created_at                                   as pushed_at,
        from_json(payload, '{{ var("push_schema") }}') as p
    from {{ ref('events') }}
    where event_type = 'PushEvent'
),

exploded as (
    select
        c.sha                                        as commit_sha,
        push.repo_name,
        push.pushed_by,
        regexp_replace(p.ref, '^refs/heads/', '')    as branch,
        c.author.name                                as author_name,
        c.author.email                               as author_email,
        c.message                                    as message,
        c.`distinct`                                 as is_distinct, 
        push.pushed_at,
        push.event_id
    from push
    lateral view explode(p.commits) as c
),

enriched as (
    select
        commit_sha,
        repo_name,
        pushed_by,
        branch,
        author_name,
        author_email,
        message,
        is_distinct,
        pushed_at,
        (message like 'Merge %')             as is_merge_commit,
        substring_index(message, '\n', 1)    as message_subject, 
        length(message)                      as message_length,
        event_id
    from exploded
),

deduped as (
    select
        *,
        row_number() over (
            partition by commit_sha
            order by pushed_at, event_id            
        ) as _rn
    from enriched
)

select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    is_merge_commit,
    message_subject,
    message_length
from deduped
where _rn = 1

