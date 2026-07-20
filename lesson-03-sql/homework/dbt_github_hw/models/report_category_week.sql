-- =====================================================================
-- TASK 7 — report_category_week (20 балів). Специфікація: ../../MODELS.md → «report_category_week».
--
-- Поряд лежить report_category_week_naive.sql — він НАВМИСНО неоптимізований:
-- join до calendar по strftime(event_date) = strftime(day) перетворює ключ join,
-- через що DuckDB сканує всі 14 партицій (немає ні propagation, ні partition pruning).
--
-- Ваша задача: переписати ТОЙ САМИЙ запит так, щоб він повертав ІДЕНТИЧНІ рядки,
-- але читав лише 7 партицій. Підказка у MODELS.md (join по сирій партиційній колоні).
-- Перевірте план: EXPLAIN ANALYZE на скомпільованій моделі → «Total Files Read».
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT
    NULL::BIGINT  AS iso_week,
    NULL::VARCHAR AS category,
    NULL::BIGINT  AS events
WHERE false  -- TODO: оптимізований варіант report_category_week_naive (join по e.event_date = c.day)
