-- =====================================================================
-- TASK 2 — repo_top_events (12 балів). Специфікація: ../../MODELS.md → «repo_top_events».
-- TOP-5 репозиторіїв за кількістю подій у кожному event_type: ROW_NUMBER() + QUALIFY.
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT
    NULL::VARCHAR AS event_type,
    NULL::VARCHAR AS repo_name,
    NULL::BIGINT  AS event_count,
    NULL::BIGINT  AS type_rank
WHERE false  -- TODO: агрегувати stg_events по (event_type, repo_name), ROW_NUMBER() OVER (...), QUALIFY type_rank <= 5
