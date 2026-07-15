# Контракти даних — GitHub Archive ETL

Це **специфікація** домашнього завдання. Для кожного етапу описано: який файл/директорію
треба створити, точну схему (назви колонок + типи), правила трансформації та
**контрольні числа** (checkpoints). Тести (`tests/test_outputs.py`) перевіряють саме ці
артефакти у директорії `data/`, а не ваш код.

Усі числа детерміновані: джерело — **незмінна** година подій GitHub
`2024-01-15-14.json.gz` (15 січня 2024, 14:00–14:59 UTC, ~267k подій).

> Типи вказані в нотації Polars. `Datetime(us, UTC)` означає
> `pl.Datetime(time_unit="us", time_zone="UTC")`.

---

## Сире джерело (надано)

Етап `landing` (`pipeline/landing.py`) уже реалізований: він **ідемпотентно** завантажує
годинний файл у `data/landing/2024-01-15-14.json.gz` (повторний запуск пропускає
завантаження). Один рядок файлу = одна подія у форматі JSON. Поле `payload`
велике й різнорідне — тому в `config.LANDING_SCHEMA` оголошено **вузьку схему читання**:
Polars читає лише потрібні поля й ігнорує решту.

---

## Завдання 1 — `bronze` (15 балів)

**Вихід:** `data/bronze/events.parquet` (один файл).
**Правило:** розгорни кожну подію у пласку таблицю. **Жодної фільтрації** — bronze містить
усі події години.

| Колонка | Тип | Джерело |
|---|---|---|
| `event_id` | `String` | `id` |
| `event_type` | `String` | `type` |
| `actor_id` | `Int64` | `actor.id` |
| `actor_login` | `String` | `actor.login` |
| `repo_id` | `Int64` | `repo.id` |
| `repo_name` | `String` | `repo.name` |
| `created_at` | `Datetime(us, UTC)` | `created_at` (парсинг ISO `...Z`) |
| `public` | `Boolean` | `public` |
| `action` | `String` (nullable) | `payload.action` |
| `commit_count` | `Int64` | довжина `payload.commits`, відсутні → `0` |

**Checkpoint:** `267 250` рядків; типів подій `> 5`; у `created_at` немає `null`.

---

## Завдання 2 — `silver` (25 балів)

**Вихід:** `data/silver/events.parquet` (один файл).
**Правило:** очисти bronze за правилами якості даних:

1. залиште лише типи з `config.TARGET_EVENT_TYPES`
   (`PushEvent`, `PullRequestEvent`, `IssuesEvent`, `WatchEvent`, `IssueCommentEvent`);
2. приберіть рядки, де `repo_name` порожній/`null`, або `event_id`/`created_at` = `null`;
3. гарантуйте **унікальність** по `event_id`.

**Схема:** ідентична `bronze` (ті самі 10 колонок і типи).
**Checkpoint:** `211 466` рядків; рівно 5 типів подій; 0 `null` у ключах; усі `event_id` унікальні.

Розподіл по типах (для самоперевірки): PushEvent `165 837`, PullRequestEvent `18 980`,
IssueCommentEvent `12 281`, WatchEvent `9 769`, IssuesEvent `4 599`.

---

## Завдання 3 — `silver` партиціонований (15 балів)

**Вихід:** `data/silver/events_by_type/` — [Hive-партиціонований](https://duckdb.org/docs/lts/data/partitioning/hive_partitioning) датасет за `event_type`.
**Правило:** запиши той самий silver, але розбитий на директорії за типом події.

Очікувані директорії:
```
events_by_type/event_type=PushEvent/...
events_by_type/event_type=PullRequestEvent/...
events_by_type/event_type=IssuesEvent/...
events_by_type/event_type=WatchEvent/...
events_by_type/event_type=IssueCommentEvent/...
```
**Checkpoint:** 5 партицій; при читанні з `hive_partitioning=True` сума рядків = `211 466`.

---

## Завдання 4 — `gold` repo_activity (15 балів)

**Вихід:** `data/gold/repo_activity.parquet`.
**Правило:** агрегація silver по `repo_name`. Відсортуйте за `event_count` спадно.

| Колонка | Тип |
|---|---|
| `repo_name` | `String` |
| `event_count` | `Int64` |
| `distinct_event_types` | `Int64` |

**Checkpoint:** `67 143` рядки; `sum(event_count) = 211 466`; відсортовано спадно.

---

## Завдання 5 — `gold` activity_per_minute (15 балів)

**Вихід:** `data/gold/activity_per_minute.parquet`.
**Правило:** часовий ряд — кількість подій silver по хвилинах
(`created_at` → `.dt.truncate("1m")`).

| Колонка | Тип |
|---|---|
| `minute` | `Datetime(us, UTC)` |
| `event_count` | `Int64` |

**Checkpoint:** `60` рядків (повна година); `sum(event_count) = 211 466`.

---

## Завдання 6 — `gold` push_commits_by_repo (15 балів)

**Вихід:** `data/gold/push_commits_by_repo.parquet`.
**Правило:** лише `PushEvent`. Агрегація по `repo_name`.

| Колонка | Тип |
|---|---|
| `repo_name` | `String` |
| `push_events` | `Int64` (кількість пушів) |
| `total_commits` | `Int64` (сума `commit_count`) |

**Checkpoint:** `52 759` рядків; `sum(total_commits) = 232 047`.

---

## +10 балів (бонус) — модульність коду

Бонус нараховується, якщо рішення **не** є «спагеті в одному файлі»: кожен етап
(`bronze` / `silver` / `gold`) — в окремому модулі з функціями, без копіпасту шляхів
і списків типів (їх беруть з `config.py`). Структура стартера вже задає цей поділ —
зберігайте його.
