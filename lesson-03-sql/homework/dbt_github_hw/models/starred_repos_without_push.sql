-- =====================================================================
-- TASK 5 — starred_repos_without_push (12 балів). Специфікація: ../../MODELS.md → «starred_repos_without_push».
-- Репозиторії зі зіркою (WatchEvent), але без жодного PushEvent: anti-join (NOT EXISTS).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT
    NULL::VARCHAR AS repo_name
WHERE false  -- TODO: репо з WatchEvent мінус репо, що мають PushEvent, у stg_events
