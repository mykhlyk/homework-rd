# dbt_taxi — Teacher Runbook (build from scratch)

Інструкторський confidence kit для dbt-блоку заняття 03.
Студенти цей файл **не бачать** — він для підготовки до live demo, де ми будуємо
проєкт **з нуля** на очах у студентів: від `dbt init` до `dbt build` + docs.

Готовий проєкт уже лежить поруч як reference. Перед demo краще зібрати його
заново у тимчасовій папці (`/tmp/dbt_demo`), щоб відрепетирувати кожен крок.

---

## Словник dbt — пояснити ПЕРЕД тим, як писати перший файл

**dbt = SQL as code, що будується як DAG.** Уся робота — це декілька типів
сутностей, які dbt складає у граф залежностей і виконує у правильному порядку.

### model
SQL-файл у `models/`. Усередині — звичайний `SELECT`. dbt сам загортає його у
`CREATE TABLE AS …` або `CREATE VIEW AS …` і виконує. Студент пише **тільки
логіку** (`SELECT`), а DDL (`CREATE`, порядок, залежності) бере на себе dbt.
Один файл = одна модель = один об'єкт у БД. Ім'я файлу = ім'я таблиці.

### `ref()`
Посилання однієї моделі на іншу: `FROM {{ ref('bronze_yellow_trips') }}`.
Це **не просто підстановка імені** — це оголошення залежності. dbt будує з усіх
`ref()` орієнтований граф (DAG) і визначає порядок виконання сам. Аналогія:
`ref()` — як `import` у Python. Циклічну залежність dbt не дозволить.
Чому не хардкодити ім'я таблиці? Бо тоді dbt не знатиме порядок, і при зміні
середовища (dev/prod) ім'я не підставиться автоматично.

### `source()`
Посилання на **зовнішні** дані, які dbt **не будує**: `{{ source('nyc_tlc',
'yellow_trips_raw') }}`. Це таблиця/файл, що вже існує (Parquet, S3, таблиця з
іншої системи). dbt лише **відстежує** її в lineage як кореневий вузол графа,
але ніколи не створює і не змінює. Оголошується у `sources.yml`.

### seed
**Маленький CSV-файл у `seeds/`, який dbt завантажує у таблицю** командою
`dbt seed`. Це reference-дані, які ти **тримаєш руками** і версіонуєш разом із
кодом. Класика — довідник `taxi_zone_lookup.csv` (zone ID → borough / zone name).

Головне — побачити різницю між трьома способами, якими дані потрапляють у проєкт:

| Сутність | Де лежать дані | Хто їх веде | Що робить dbt |
|---|---|---|---|
| **source** | вже в БД / зовні (Parquet, S3) | upstream-система | лише **посилається** (`source()`) |
| **seed** | CSV-файл **у репозиторії** | **ти**, вручну | **завантажує** (`dbt seed`) у таблицю |
| **model** | обчислюється з інших таблиць | dbt, за твоїм SQL | **будує** (`dbt run`) |

Питання-фільтр для seed: *«Це маленька довідкова таблиця, яку я веду руками і
хочу тримати під version control поруч із кодом?»* Якщо так → seed.

- **Що годиться у seed:** lookup / mapping (zone → borough), коди країн/валют,
  status-code → label, список тестових акаунтів на виключення, календар свят.
- **Що НЕ годиться:** самі дані (3 млн поїздок — це **source**, не seed!),
  будь-що велике (dbt вантажить seed через `INSERT` — повільно), будь-що
  обчислюване (це **model**).
- **Чому CSV, а не модель із `VALUES`?** git diff показує, коли і хто змінив
  довідник; CSV редагує і не-інженер; а в DAG seed поводиться як звичайний вузол —
  на нього посилаються тим самим `{{ ref('taxi_zone_lookup') }}`, що й на модель.
- **Пастка з назвою:** «seed» тут **не** має стосунку до random seed чи
  «заповнення БД фейковими даними». У dbt це конкретно «CSV-довідник у проєкті».

### test
SQL-запит, що **перевіряє інваріант даних**. Логіка вивернута: тест повертає
рядки, які **порушують** правило. **0 рядків = тест пройшов.** Два види:
- **schema test** — декларативний, у `schema.yml` (`not_null`, `unique`,
  `accepted_values`, `relationships`). dbt сам генерує SQL.
- **singular test** — довільний `.sql` у `tests/`. Пишеш запит «знайди погані
  рядки» вручну (напр. `WHERE fare_amount < 0`).

### materialization (`table` vs `view`)
Як саме dbt матеріалізує модель:
- **`view`** — збережений запит, виконується щоразу при зверненні; місця на
  диску не займає. Дешевий шар фільтрації/перейменування.
- **`table`** — фізично записані дані на диск; швидке читання, але треба
  перебудовувати. Для агрегацій і snapshot вхідних даних.

Задається через `{{ config(materialized="…") }}` у моделі або дефолтом у
`dbt_project.yml`. Це **той самий вибір table-vs-view**, що в секції 8 ноутбука —
тільки тут він керований і задокументований.

---

## Крок 0 — оточення (один раз, до заняття)

```bash
mkdir -p /tmp/dbt_demo && cd /tmp/dbt_demo
uv venv --python 3.12
source .venv/bin/activate            # macOS/Linux
uv pip install dbt-duckdb --python .venv/bin/python
```

> **Python 3.12 навмисно.** dbt-duckdb (через mashumaro) ще несумісний з 3.14+.
> Якщо `dbt` не бачить адаптер duckdb — майже завжди це різні середовища:
> `dbt` з PATH і `dbt-duckdb` встановлено в інший env. `dbt init` бере список
> адаптерів із того ж Python, у якому він запущений. Запускайте через активований
> `.venv` (або `uv run dbt …`).

---

## Крок 1 — `dbt init`

```bash
dbt init dbt_taxi
```

dbt запитає адаптер — обрати **duckdb** (якщо встановлено лише його, підставиться сам).
Створиться скелет:

```
dbt_taxi/
├── dbt_project.yml
├── models/example/      ← приклади, які ми зараз видалимо
└── ...
```

`dbt init` за замовчуванням пише профіль у `~/.dbt/profiles.yml`. Ми натомість
хочемо **локальний** профіль поруч із проєктом — щоб шлях до БД і код жили разом.

```bash
cd dbt_taxi
rm -rf models/example          # приклади не потрібні
```

---

## Крок 2 — `profiles.yml` (локальний, у папці проєкту)

Створити `profiles.yml`:

```yaml
dbt_taxi:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: dbt_taxi.duckdb
      extensions:
        - httpfs          # щоб читати remote Parquet із CloudFront
```

> Запускати dbt будемо з `--profiles-dir .`, бо профіль лежить тут, а не в `~/.dbt`.

---

## Крок 3 — `dbt_project.yml`

Замінити згенерований вміст на:

```yaml
name: "dbt_taxi"
version: "1.0.0"
config-version: 2

profile: "dbt_taxi"

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

# Hook перед будь-якою командою: створює view на remote Parquet,
# у який резолвиться source('nyc_tlc', 'yellow_trips_raw').
on-run-start:
  - "CREATE OR REPLACE VIEW yellow_trips_raw AS SELECT * FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet')"

models:
  dbt_taxi:
    +materialized: view      # дефолт — view; матеріалізуємо точково
```

> **Пояснити `on-run-start`:** у заняття-ноутбуці ми читали Parquet напряму
> через `read_parquet(...)`. У dbt source має бути об'єктом у БД, тож hook
> створює view-обгортку перед кожним запуском.

```bash
dbt debug --profiles-dir .
```

Очікувано: `adapter type: duckdb`, `Connection test: OK`, `All checks passed!`.

---

## Крок 4 — `source` (declare, not build)

`models/sources.yml`:

```yaml
version: 2

sources:
  - name: nyc_tlc
    schema: main
    description: "NYC TLC Yellow Taxi Trip Records, January 2024 (remote Parquet via httpfs)"
    tables:
      - name: yellow_trips_raw
        description: "Raw trip records — view created via on-run-start hook from CloudFront Parquet"
```

> dbt цей об'єкт **не будує** — лише посилається на нього в lineage через `source()`.

---

## Крок 5 — Bronze model (типізація)

`models/bronze/bronze_yellow_trips.sql`:

```sql
{{ config(materialized="table") }}

SELECT
    CAST(VendorID              AS INTEGER)       AS vendor_id,
    CAST(tpep_pickup_datetime  AS TIMESTAMP)     AS tpep_pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP)     AS tpep_dropoff_datetime,
    CAST(passenger_count       AS SMALLINT)      AS passenger_count,
    CAST(trip_distance         AS DOUBLE)        AS trip_distance,
    CAST(RatecodeID            AS SMALLINT)      AS ratecode_id,
    CAST(store_and_fwd_flag    AS VARCHAR)       AS store_and_fwd_flag,
    CAST(PULocationID          AS SMALLINT)      AS pu_location_id,
    CAST(DOLocationID          AS SMALLINT)      AS do_location_id,
    CAST(payment_type          AS SMALLINT)      AS payment_type,
    CAST(fare_amount           AS DECIMAL(10,2)) AS fare_amount,
    CAST(extra                 AS DECIMAL(10,2)) AS extra,
    CAST(mta_tax               AS DECIMAL(10,2)) AS mta_tax,
    CAST(tip_amount            AS DECIMAL(10,2)) AS tip_amount,
    CAST(tolls_amount          AS DECIMAL(10,2)) AS tolls_amount,
    CAST(improvement_surcharge AS DECIMAL(10,2)) AS improvement_surcharge,
    CAST(total_amount          AS DECIMAL(10,2)) AS total_amount,
    CAST(congestion_surcharge  AS DECIMAL(10,2)) AS congestion_surcharge,
    CAST("Airport_fee"         AS DECIMAL(10,2)) AS airport_fee
FROM {{ source("nyc_tlc", "yellow_trips_raw") }}
```

> `{{ config(materialized="table") }}` перебиває дефолт-view: bronze — це
> матеріалізований snapshot. Ті самі типи, що в DDL із секції 3 ноутбука,
> але **без CHECK** — DQ-фільтрацію винесли в Silver.

```bash
dbt run --select bronze_yellow_trips --profiles-dir .
```

---

## Крок 6 — Silver model (фільтрація через `ref()`)

`models/silver/stg_yellow_trips.sql`:

```sql
{{ config(materialized="view") }}

SELECT *
FROM {{ ref("bronze_yellow_trips") }}
WHERE fare_amount           >= 0
  AND trip_distance         >= 0
  AND tpep_pickup_datetime  IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND pu_location_id        IS NOT NULL
  AND do_location_id        IS NOT NULL
```

> Перший `ref()` — показати студентам, що bronze у `FROM` не хардкодимо.
> Це **view** — дешевий шар фільтрації, без місця на диску.

---

## Крок 7 — Seed (довідник зон)

Покласти `seeds/taxi_zone_lookup.csv` (265 рядків, TLC reference).

```bash
dbt seed --profiles-dir .
# 1 of 1 OK loaded seed file main.taxi_zone_lookup [INSERT 265 in 0.04s]
```

> Seed потрібен **до** `zone_revenue` (той робить join) і до `relationships`-тесту.
> dbt бачить seed як upstream-вузол DAG і вантажить його першим.

---

## Крок 8 — Аналітична model (window functions)

`models/silver/zone_revenue.sql`:

```sql
{{ config(materialized="table") }}

WITH trip_totals AS (
    SELECT
        pu_location_id,
        count(*)         AS trip_count,
        sum(fare_amount) AS total_revenue,
        avg(fare_amount) AS avg_fare,
        sum(tip_amount)  AS total_tips
    FROM {{ ref("stg_yellow_trips") }}
    GROUP BY pu_location_id
),
with_zones AS (
    SELECT
        t.pu_location_id,
        z.Borough    AS borough,
        z.Zone       AS zone,
        z.service_zone,
        t.trip_count, t.total_revenue, t.avg_fare, t.total_tips
    FROM trip_totals t
    JOIN {{ ref("taxi_zone_lookup") }} z ON t.pu_location_id = z.LocationID
)
SELECT
    *,
    SUM(total_revenue) OVER (PARTITION BY borough ORDER BY pu_location_id)
                                AS cumulative_borough_revenue,
    RANK() OVER (PARTITION BY borough ORDER BY total_revenue DESC)
                                AS revenue_rank_in_borough
FROM with_zones
```

> Той самий `SUM OVER (PARTITION BY ...)` і `RANK()`, що в секції 4 ноутбука —
> тепер як модель із двома `ref()`: на model (`stg`) і на seed (`taxi_zone_lookup`).
> Зверни увагу: синтаксис `ref()` однаковий для моделі й для seed.

---

## Крок 9 — Тести

`models/schema.yml` — schema-тести (dbt сам генерує SQL):

```yaml
version: 2

models:
  - name: bronze_yellow_trips
    description: "Типізована Bronze-таблиця. Явні CAST для всіх колонок."
    columns:
      - name: tpep_pickup_datetime
        tests: [not_null]
      - name: tpep_dropoff_datetime
        tests: [not_null]
      - name: pu_location_id
        tests: [not_null]
      - name: do_location_id
        tests: [not_null]
      - name: payment_type
        tests:
          - accepted_values:
              values: [0, 1, 2, 3, 4]

  - name: stg_yellow_trips
    description: "Silver view — відфільтровані поїздки."
    columns:
      - name: pu_location_id
        tests:
          - relationships:
              to: ref('taxi_zone_lookup')
              field: LocationID
              config:
                severity: warn

  - name: zone_revenue
    description: "Аналітична таблиця: виручка по зонах із window functions."
```

`tests/assert_fare_non_negative.sql` — singular test (довільний SQL):

```sql
-- Має повернути 0 рядків. stg фільтрує fare_amount >= 0, тож завжди зелений.
SELECT *
FROM {{ ref("stg_yellow_trips") }}
WHERE fare_amount < 0
```

> **Ключова теза:** singular test перевіряє **stg**, а не bronze — після фільтра
> він завжди зелений. На bronze був би червоним. Тест прив'язаний до шару, де
> інваріант має виконуватись.

---

## Крок 10 — Запустити все разом

```bash
dbt build --profiles-dir .          # seed + run + test одним DAG
# Done. PASS=12 WARN=0 ERROR=0 SKIP=0 TOTAL=12
```

Порядок dbt визначає сам за `ref()`:
`bronze → stg → zone_revenue`, а seed і source — як upstream-вузли.

Окремі команди, якщо треба показати поетапно:

```bash
dbt seed  --profiles-dir .          # 265 рядків довідника
dbt run   --profiles-dir .          # 3 моделі у порядку ref()
dbt test  --profiles-dir .          # 7 тестів (4×not_null, accepted_values, relationships, singular)
```

> **На що звернути увагу:** bronze — `table` (на диску), stg — `view` (збережений
> запит). Та сама різниця, що в секції 8 ноутбука, але тепер у керованому pipeline.

---

## Крок 11 — Документація та lineage

```bash
dbt docs generate --profiles-dir .
dbt docs serve                       # http://localhost:8080, Ctrl+C щоб зупинити
```

У **Lineage Graph** — DAG:

```
[source: nyc_tlc.yellow_trips_raw]
         ↓
[bronze_yellow_trips]   [seed: taxi_zone_lookup]
         ↓                        ↓
[stg_yellow_trips] ───→ [zone_revenue] ←┘
```

> Кожен вузол — файл у проєкті. Колір залежить від типу сутності: `source`
> (зовнішній Parquet) і `seed` (CSV) окремими кольорами від `model`.

### Тести в lineage — як прибрати «шум»

Тести (`assert_fare_non_negative`, `not_null`, `relationships` тощо) — це теж
**вузли DAG**, тож вони з'являються в графі поряд із моделями і часто його
засмічують. Через CLI відфільтрувати їх допомагають **graph operators** —
селектори, де `+` означає «вгору/вниз по залежностях»:

```bash
dbt ls --select stg_yellow_trips+            # модель і все downstream
dbt ls --select +zone_revenue                # все upstream від моделі
dbt ls --select +zone_revenue+               # повне оточення (upstream + downstream)
dbt ls --select stg_yellow_trips+ --exclude resource_type:test   # без тест-вузлів
dbt ls --resource-type model                 # лише моделі, нічого іншого
```

Ті самі селектори працюють у `dbt run` / `build` / `test`, тож можна не лише
дивитися граф, а й запускати точково:

```bash
dbt build --select +fact_trip --exclude resource_type:test
```

> **Важливий нюанс:** CLI-селектори керують тим, **що команда обробляє**
> (`ls`/`run`/`build`/`test`), а не тим, що малює сайт `dbt docs`. Сам сайт
> завжди показує **повний** DAG із manifest. Прибрати тести з картинки на сайті —
> це окремо, через node-selector у самому UI (зняти галку «Test»), а не прапорцем CLI.

---

## Маппінг model ↔ notebook-секція

| dbt model | Секція ноутбука | Що спільного |
|---|---|---|
| `bronze_yellow_trips` | 3 (DDL Bronze) | Ті самі типи (DECIMAL, SMALLINT, TIMESTAMP), той самий набір колонок |
| `stg_yellow_trips` | 3 (filtered INSERT) | Та сама логіка: `fare_amount >= 0 AND trip_distance >= 0` |
| `zone_revenue` | 4 (window functions) | Ті самі `SUM OVER (PARTITION BY borough)` і `RANK()` |

> **Теза:** "Той самий SQL, що ми писали руками — тепер у файлах, з порядком
> виконання, тестами і документацією. Оце і є dbt."

---

## FAQ — часті питання студентів

**Q: Чому окремий `dbt_taxi.duckdb`, а не `taxi_dwh.duckdb` з ноутбука?**
A: Навмисно різні файли. `taxi_dwh.duckdb` — "production DB" з ноутбука;
`dbt_taxi.duckdb` — ізольоване dbt-середовище. DuckDB — single-writer на файл.

**Q: Що таке `on-run-start`?**
A: Hook перед будь-якою командою dbt. Створює `yellow_trips_raw` view, щоб
`{{ source(...) }}` у bronze резолвився у нього.

**Q: Чому `stg` — view, а не table?**
A: View — дешевий шар фільтрації, без місця на диску. Матеріалізуємо лише там,
де агрегація (`zone_revenue`) або потрібен snapshot (`bronze`).

**Q: Чим seed відрізняється від source?**
A: Seed dbt **завантажує** з CSV у репозиторії (ти ведеш його руками). Source dbt
**не чіпає** — лише посилається на вже наявні зовнішні дані. Довідник зон —
маленький і наш → seed. 3 млн поїздок — великі й зовнішні → source.

**Q: Що перевіряє `relationships`-тест?**
A: Referential integrity: кожен `pu_location_id` у stg має існувати в
`taxi_zone_lookup.LocationID`. З'являться поїздки з невідомих зон — тест попередить.

**Q: Звідки береться документація?**
A: З `description:` у `schema.yml` + структура з `ref()`/`source()`. dbt збирає
все у `dbt docs`.

---

## Live-failure playbook

### `dbt init` не пропонує duckdb / "Could not find adapter"
Різні Python-середовища: `dbt` з PATH ≠ env із `dbt-duckdb`. Активуйте `.venv`
або `uv run dbt …`. Перевірка: `dbt --version` має показати `duckdb: 1.x` у плагінах.

### Немає інтернету
`on-run-start` зафейлиться на `CREATE VIEW yellow_trips_raw` (немає доступу до
CloudFront). Фікс — підмінити шлях на локальний файл:
```sql
-- у dbt_project.yml:
-- "CREATE OR REPLACE VIEW yellow_trips_raw AS SELECT * FROM read_parquet('../../lesson-02-python/code/data/landing/yellow_tripdata_2024-01.parquet')"
```
Або показати `dbt compile --profiles-dir .` — генерує SQL без запуску.

### `Could not find profile named 'dbt_taxi'`
Забули `--profiles-dir .`. Профіль лежить у папці проєкту навмисно.

### `Database is locked`
Хтось відкрив `dbt_taxi.duckdb` у DBeaver без read-only паралельно з `dbt build`.
Фікс: `?access_mode=read_only` у DBeaver або закрити з'єднання.

### Перший `dbt build` повільний
bronze читає ~100 MB remote Parquet через CloudFront — 10–30 сек залежно від
мережі. Без `--full-refresh` повторний запуск пропустить готові таблиці.

### `mashumaro.exceptions.UnserializableField`
dbt-duckdb несумісний з Python 3.14+. Перестворити env на 3.12:
```bash
rm -rf .venv && uv venv --python 3.12 && uv pip install dbt-duckdb --python .venv/bin/python
```
