-- Перевірка task 1: у stg_events не лишилось «брудних» рядків.
-- Має повертати 0 рядків.
SELECT *
FROM {{ ref('stg_events') }}
WHERE event_type NOT IN ('PushEvent', 'IssuesEvent', 'PullRequestEvent', 'WatchEvent', 'IssueCommentEvent')
   OR actor_login LIKE '%[bot]'
   OR (event_type = 'PushEvent' AND payload_commit_count = 0)
