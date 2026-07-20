-- Перевірка task 7: оптимізований report_category_week має повертати ТОЧНО ті самі рядки,
-- що й report_category_week_naive. Оптимізація змінює лише план виконання, не результат.
-- Симетрична різниця множин має бути порожня → 0 рядків.
SELECT * FROM (
    SELECT * FROM {{ ref('report_category_week') }}
    EXCEPT
    SELECT * FROM {{ ref('report_category_week_naive') }}
)
UNION ALL
SELECT * FROM (
    SELECT * FROM {{ ref('report_category_week_naive') }}
    EXCEPT
    SELECT * FROM {{ ref('report_category_week') }}
)
