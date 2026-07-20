-- =====================================================================
-- TASK 4 — daily_activity_change (12 балів). Специфікація: ../../MODELS.md → «daily_activity_change».
-- Зміна кількості подій день-до-дня: LAG(...) OVER (ORDER BY ...).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT
    NULL::DATE   AS event_date,
    NULL::BIGINT AS events,
    NULL::BIGINT AS prev_day_events,
    NULL::BIGINT AS delta_events
WHERE false  -- TODO: агрегувати stg_events по event_date, потім LAG для попереднього дня
