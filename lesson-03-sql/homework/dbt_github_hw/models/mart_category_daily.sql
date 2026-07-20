-- =====================================================================
-- TASK 6 — mart_category_daily (20 балів). Специфікація: ../../MODELS.md → «mart_category_daily».
-- Широка вітрина: multi-join stg_events + event_categories + calendar, агрегація по (день × категорія).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT
    NULL::DATE    AS event_date,
    NULL::BOOLEAN AS is_weekend,
    NULL::VARCHAR AS category,
    NULL::BIGINT  AS events,
    NULL::BIGINT  AS distinct_repos,
    NULL::BIGINT  AS distinct_actors
WHERE false  -- TODO: 3-way join + GROUP BY (event_date, is_weekend, category)
