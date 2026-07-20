-- Перевірка task 3: накопичувальна кількість подій не може спадати.
-- Якщо running_events десь менший за попередній рядок — running total порахований неправильно.
-- Має повертати 0 рядків.
WITH r AS (
    SELECT
        event_date,
        running_events,
        lag(running_events) OVER (ORDER BY event_date) AS prev_running_events
    FROM {{ ref('daily_activity') }}
)
SELECT *
FROM r
WHERE prev_running_events IS NOT NULL
  AND running_events < prev_running_events
