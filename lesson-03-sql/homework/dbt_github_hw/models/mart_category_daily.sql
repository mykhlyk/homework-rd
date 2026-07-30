-- =====================================================================
-- TASK 6 — mart_category_daily (20 балів). Специфікація: ../../MODELS.md → «mart_category_daily».
-- Широка вітрина: multi-join stg_events + event_categories + calendar, агрегація по (день × категорія).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================

SELECT
    e.event_date,
    c.is_weekend,
    cat.category,
    COUNT(*) AS events,
    COUNT(DISTINCT e.repo_name) AS distinct_repos,
    COUNT(DISTINCT e.actor_login) AS distinct_actors
FROM {{ ref('stg_events') }} e
JOIN {{ ref('event_categories') }} cat
    ON e.event_type = cat.event_type
JOIN {{ ref('calendar') }} c
    ON e.event_date = c.day
GROUP BY
    e.event_date,
    c.is_weekend,
    cat.category