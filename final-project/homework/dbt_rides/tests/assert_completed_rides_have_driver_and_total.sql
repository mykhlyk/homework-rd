-- Власний тест 1. Завершена поїздка мусить мати водія і підсумкову суму: вона пройшла
-- accept -> start -> complete, а ride_completed несе driver.id і fare.total. Ловить: втрату
-- driver_id при згортці або зникнення суми (напр. якщо fare.total не дотягнувся з payload).
select ride_id, driver_id, total_amount
from {{ ref('rides') }}
where status = 'completed'
  and (driver_id is null or total_amount is null)
