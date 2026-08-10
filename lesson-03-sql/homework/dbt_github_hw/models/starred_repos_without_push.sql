-- =====================================================================
-- TASK 5 — starred_repos_without_push (12 балів). Специфікація: ../../MODELS.md → «starred_repos_without_push».
-- Репозиторії зі зіркою (WatchEvent), але без жодного PushEvent: anti-join (NOT EXISTS).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT DISTINCT e1.repo_name
FROM {{ ref('stg_events') }} e1
WHERE e1.event_type = 'WatchEvent'
  AND NOT EXISTS (
      SELECT 1
      FROM {{ ref('stg_events') }} e2
      WHERE e2.repo_name = e1.repo_name
        AND e2.event_type = 'PushEvent'
  )