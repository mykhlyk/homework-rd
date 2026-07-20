# MODELS.md — специфікація моделей

Це головний документ домашнього завдання. Тут описано **що саме** має повертати кожна
модель dbt. Ваша задача — реалізувати кожну модель у `dbt_github_hw/models/` так, щоб
вона відповідала специфікації. Логіку запиту ви пишете самі — нижче лише контракт
(колонки, патерн, очікуваний результат).

> **Checkpoint** у кожному завданні — це орієнтовна кількість рядків / контрольне значення
> на нашому датасеті. Якщо ваша модель повертає інше число — десь помилка. Заглушка
> повертає 0 рядків (тести проходять «вхолосту»), тож звіряйтеся саме з checkpoint.

## Дані

Це продовження ДЗ заняття 02: ті самі **GitHub Archive events**, але вже у вигляді
партиційованого Parquet. Усі моделі будуються поверх трьох джерел:

| Джерело | Що це | Як читати |
|---|---|---|
| `read_parquet('{{ var("events_path") }}', hive_partitioning = true)` | Сирі події GitHub за 1–14 січня 2024, по одній годині на день, партиційовано по `event_date` (14 партицій) | напряму у `stg_events` |
| `{{ ref('event_categories') }}` | Довідник (seed): `event_type` → `category` (`code` / `issues` / `social`), `is_write` | через `ref()` |
| `{{ ref('calendar') }}` | Календар (seed): `day`, `day_of_week`, `is_weekend`, `iso_week` | через `ref()` |

Сирі колонки подій: `id`, `event_type`, `created_at`, `event_date`, `actor_login`,
`repo_name`, `payload_ref`, `payload_commit_count`, `payload_action`.

**Сирі дані «брудні» навмисно:** містять усі 15 типів подій, події ботів
(`actor_login` на кшталт `dependabot[bot]`) і «порожні» push-и без комітів — це чистить
`stg_events`.

---

## Task 1 — `stg_events` · 12 балів

**Що:** чистий шар поверх сирих подій (schema-on-read). Прибирає шум, який завалює аналітику.

**Колонки на виході:** `id`, `event_type`, `created_at`, `event_date`, `actor_login`,
`repo_name`, `payload_commit_count`, `payload_action`, `payload_ref`.

**DQ-фільтри (лишити лише рядки, де):**
- `event_type` ∈ `PushEvent`, `IssuesEvent`, `PullRequestEvent`, `WatchEvent`, `IssueCommentEvent`;
- `actor_login` **не** закінчується на `[bot]` (прибрати ботів);
- **не** «порожній» push: прибрати рядки, де `event_type = 'PushEvent'` і `payload_commit_count = 0`.

**Матеріалізація:** `view` (важливо для Task 7 — щоб partition pruning працював наскрізь;
не додавайте сюди window-функцій, вони блокують pruning).

**Checkpoint:** із **101 833** сирих рядків лишається **≈68 158**.

---

## Task 2 — `repo_top_events` · 12 балів

**Що:** TOP-5 репозиторіїв за кількістю подій у межах кожного `event_type`.

**Колонки:** `event_type`, `repo_name`, `event_count`, `type_rank`.

**Патерн:** спочатку агрегація у CTE (`GROUP BY event_type, repo_name`), потім
`ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_count DESC, repo_name)` і
**`QUALIFY type_rank <= 5`** (фільтр по window function без підзапиту — DuckDB-спосіб).

**Checkpoint:** **25 рядків** (5 типів × 5).

---

## Task 3 — `daily_activity` · 12 балів

**Що:** кількість подій по днях + накопичувальний (running) підсумок.

**Колонки:** `event_date`, `events`, `running_events` (накопичувальна сума `events`).

**Патерн:** агрегація у CTE (`GROUP BY event_date`), далі
`SUM(events) OVER (ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.

**Checkpoint:** **14 рядків**; останній `running_events` = **68 158** (= всі рядки `stg_events`).

---

## Task 4 — `daily_activity_change` · 12 балів

**Що:** зміна кількості подій день-до-дня.

**Колонки:** `event_date`, `events`, `prev_day_events`, `delta_events` (= `events - prev_day_events`).

**Патерн:** агрегація по дню, далі `LAG(events) OVER (ORDER BY event_date)`.
Для першого дня `prev_day_events` і `delta_events` будуть `NULL` — це нормально.

**Checkpoint:** **14 рядків**.

---

## Task 5 — `starred_repos_without_push` · 12 балів

**Що:** репозиторії, які отримали зірку (`WatchEvent`), але **не мали жодного `PushEvent`**
у наборі даних — «популярні, але неактивні» репо. Anti-join: одна множина мінус інша.

**Колонки:** `repo_name`.

**Патерн:** множина репо з `WatchEvent` мінус репо, що мають `PushEvent`. Реалізуйте через
`NOT EXISTS` або `LEFT JOIN ... WHERE ... IS NULL`. **Не використовуйте `NOT IN`** з
підзапитом, що може містити `NULL`.

**Checkpoint:** **3 739 репозиторіїв**.

---

## Task 6 — `mart_category_daily` · 20 балів

**Що:** широка вітрина — об'єднує події з **двома** довідниками в одну таблицю
(це і є приклад multi-join). Грануляція: один рядок на `(event_date, category)`.

**Колонки:** `event_date`, `is_weekend`, `category`, `events` (кількість),
`distinct_repos` (`count(DISTINCT repo_name)`), `distinct_actors` (`count(DISTINCT actor_login)`).

**Патерн:** 3-way join `stg_events` + `event_categories` (по `event_type`) +
`calendar` (по `event_date = day`), далі `GROUP BY event_date, is_weekend, category`.

**Checkpoint:** **42 рядки** (14 днів × 3 категорії).

---

## Task 7 — `report_category_week` · 20 балів

**Що:** оптимізувати готовий запит. Поряд лежить **`report_category_week_naive.sql`**
(дано, не редагувати) — тижневий звіт по категоріях за `iso_week = 2`. Він написаний
**навмисно погано**: join до `calendar` відбувається по рядково-форматованій даті:

```sql
JOIN calendar c ON strftime(e.event_date, '%Y-%m-%d') = strftime(c.day, '%Y-%m-%d')
```

Через перетворення ключа join (`strftime`) оптимізатор DuckDB **не може**:
1. **пропагувати фільтр** `c.iso_week = 2` на партиційну колону `event_date`
   (*predicate / filter propagation* — фільтр з однієї таблиці «переходить» через ключ
   join на іншу);
2. виконати **partition pruning** — у результаті сканує **всі 14 партицій**.

**Ваша задача:** написати `report_category_week.sql`, що повертає **ІДЕНТИЧНІ рядки**, але
читає лише **7 партицій**. Єдина правильна зміна — прибрати перетворення ключа і робити
join по **сирій партиційній колоні**:

```sql
JOIN calendar c ON e.event_date = c.day
```

Тепер DuckDB пропагує `iso_week = 2` → `event_date` потрапляє у дні тижня 2 (8–14 січня) →
partition pruning лишає 7 партицій із 14.

**Колонки:** `iso_week`, `category`, `events` (як у naive).

**Як перевірити план (обов'язково):**
```bash
# після dbt build, з директорії проєкту:
uv run python - <<'PY'
import duckdb, pathlib
con = duckdb.connect("warehouse.duckdb", read_only=True)
for m in ["report_category_week_naive", "report_category_week"]:
    sql = pathlib.Path(f"target/compiled/dbt_github_hw/models/{m}.sql").read_text()
    plan = con.sql("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
    line = [l for l in plan.splitlines() if "Total Files Read" in l]
    print(m, "->", line)
PY
```
Маєте побачити `naive → 14`, ваш `report_category_week → 7`.

**Checkpoint:** **3 рядки** (категорії `code` / `issues` / `social`); сумарно `events` =
**35 945**. Тест `assert_report_matches_naive` має бути зеленим (рядки збігаються з naive).
