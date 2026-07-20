# Заняття 03. SQL для Data Engineering

## Навіщо це заняття

SQL — найстаріша мова роботи з даними, але в контексті Data Engineering він відіграє зовсім іншу роль, ніж у контексті застосунків. Розробник пише SQL, щоб прочитати запис із бази. DE пише SQL, щоб трансформувати мільйони записів, матеріалізувати результати у нові таблиці та побудувати pipeline, що підтримується командою протягом років.

Заняття розбите на дві великі частини. Перша — DuckDB як аналітичний SQL engine: ми переходимо від schema-on-read (заняття 02) до schema-on-write (типізовані таблиці з обмеженнями), освоюємо window functions та CTE, вчимося читати query plan через `EXPLAIN ANALYZE`, і будуємо Bronze/Silver шари вручну через Python API.

Друга частина — головний pivot заняття: **dbt**. dbt не додає нову мову і не замінює SQL. Він перетворює звичайний SQL на *code*, що тестується, версіонується і документується. Якщо перша частина показує «як це виглядає вручну», друга показує «як це виглядає у команді».

Практичний результат: DuckDB-база `taxi_dwh.duckdb` з типізованою Bronze-таблицею і Silver у Parquet, плюс перший dbt-проєкт `dbt_taxi` з Bronze, Silver і аналітичною моделями.

## Що треба знати заздалегідь

Заняття 01 (середовище), заняття 02 (schema-on-read через Parquet, pandas/Polars, landing zone — файл `yellow_tripdata_2024-01.parquet` живе в `data/source/`). Базовий SQL: `SELECT`, `JOIN`, `GROUP BY`, `ORDER BY`, `WHERE` — знайомий синтаксис передбачається.

Шляхи у коді: ноутбук запускається з директорії `code/`. Спільне джерело — `../../data/source/`. Вихід заняття — `../../data/lesson-03/`.

---

## 1. DuckDB: два режими одного інструменту

### Schema-on-read: EDA без DDL

Перший режим DuckDB — читати Parquet напряму, без жодного DDL:

```python
con.sql(f"DESCRIBE SELECT * FROM read_parquet('{LOCAL}')")
con.sql(f"SUMMARIZE SELECT * FROM read_parquet('{LOCAL}')")
```

`SUMMARIZE` — це одна команда, що повертає для кожної колонки: count, null_count, mean, std, min, max, квартилі. Це і `DESCRIBE`, і `df.describe()` pandas в одному запиті.

Ключовий момент: `min` для `fare_amount` виявляється **від'ємним**. Це перше явне підтвердження DQ-проблеми, яку ви вже побачили на занятті 02. DuckDB не «дає помилку» — він просто зчитує те, що є у файлі. Це і є schema-on-read: схема відома, але значення не перевіряються.

### Читання remote Parquet через httpfs

DuckDB вміє читати Parquet напряму з HTTP або S3, без попереднього завантаження:

```python
con.sql("INSTALL httpfs; LOAD httpfs;")
REMOTE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
con.sql(f"SELECT count(*) AS total_rows FROM read_parquet('{REMOTE}')")
```

`httpfs` робить **range-запити**: завантажується лише footer (схема) і потрібні row groups, а не весь файл. Для запиту `COUNT(*)` завантажиться лише footer з метаданими — кілобайти замість 100 MB.

---

## 2. DDL: типізована Bronze-таблиця і schema-on-write

### Навіщо DDL після schema-on-read

Schema-on-read зручний для EDA: відкрили файл, подивились. Але у production він небезпечний: schema drift (зміна структури вхідних файлів) проходить непоміченим, поки в pipeline не починаються тихі помилки.

Schema-on-write вирішує це через DDL — явне оголошення структури до завантаження:

```python
con.sql("""
CREATE OR REPLACE TABLE bronze_yellow_trips (
    vendor_id             INTEGER,
    tpep_pickup_datetime  TIMESTAMP    NOT NULL,
    tpep_dropoff_datetime TIMESTAMP    NOT NULL,
    passenger_count       SMALLINT,
    trip_distance         DOUBLE,
    pu_location_id        USMALLINT    NOT NULL,
    do_location_id        USMALLINT    NOT NULL,
    fare_amount           DECIMAL(10,2) NOT NULL,
    total_amount          DECIMAL(10,2) NOT NULL,
    airport_fee           DECIMAL(10,2),
    CHECK (fare_amount >= 0),
    CHECK (trip_distance >= 0),
    CHECK (passenger_count BETWEEN 0 AND 9 OR passenger_count IS NULL)
)
""")
```

### Чому DECIMAL для фінансів

`DOUBLE` (float64) дає IEEE 754 drift — `0.1 + 0.2 = 0.30000000000000004`. Для грошових сум це неприйнятно. `DECIMAL(10,2)` зберігає точне десяткове представлення без drift.

### Навіщо деякі колонки без NOT NULL

Не всі поля обов'язкові в реальних даних. У датасеті 2024 року `passenger_count`, `ratecode_id`, `airport_fee` і `congestion_surcharge` мають легітимні NULL — це не помилка, а факт: деякі поїздки не мають відповідних значень. Bronze-шар зберігає сирі дані як є, не прибираючи NULL.

### CHECK ловить погані дані на INSERT

Спроба вставити всі рядки (включно з `fare_amount < 0`) відкочує весь INSERT — транзакційна семантика:

```python
try:
    con.sql(f"INSERT INTO bronze_yellow_trips SELECT ... FROM read_parquet('{LOCAL}')")
except Exception as e:
    print(f"CHECK порушено — INSERT відкатано:\n{e}")

# Із фільтром — Bronze матеріалізована:
con.sql(f"""
INSERT INTO bronze_yellow_trips SELECT ... FROM read_parquet('{LOCAL}')
WHERE fare_amount >= 0 AND trip_distance >= 0
""")
```

Це DQ-перевірка вбудована прямо в storage. Немає можливості «забути» провалідувати — база сама не пропустить.

---

## 3. Window functions і CTE: відповідаємо на бізнес-питання

### Window functions vs GROUP BY

Фундаментальна різниця: `GROUP BY` **колапсує** рядки до груп — після нього ви маєте один рядок на групу. Window function **зберігає всі рядки** і додає агреговане значення поряд.

Аналогія: GROUP BY — як таблиця підсумків за місяцями. Window function — як колонка «накопичений підсумок» поряд із щоденними рядками.

### ROW_NUMBER + QUALIFY: топ-N на групу

Одне з найчастіших DE-завдань — знайти топ-N записів у кожній групі (наприклад, топ-5 найдорожчих поїздок із кожної зони).

`QUALIFY` — DuckDB-синтаксис (також є у BigQuery і Snowflake) для фільтрації за результатом window function прямо у `SELECT`, без вкладеного підзапиту:

```sql
SELECT
    pu_location_id,
    tpep_pickup_datetime,
    fare_amount,
    total_amount,
    ROW_NUMBER() OVER (PARTITION BY pu_location_id ORDER BY total_amount DESC) AS rank_in_zone
FROM bronze_yellow_trips
QUALIFY rank_in_zone <= 5
ORDER BY pu_location_id, rank_in_zone;
```

Без `QUALIFY` довелося б загортати весь запит у підзапит і фільтрувати знадвору. `QUALIFY` робить це в одному рівні.

### Накопичувальна сума і LAG: часові ряди

```sql
-- Кумулятивна виручка по годині доби
WITH hourly AS (
    SELECT datepart('hour', tpep_pickup_datetime) AS hour_of_day,
           sum(fare_amount) AS hourly_revenue
    FROM bronze_yellow_trips
    GROUP BY 1
)
SELECT
    hour_of_day,
    hourly_revenue,
    SUM(hourly_revenue) OVER (ORDER BY hour_of_day) AS cumulative_revenue
FROM hourly
ORDER BY hour_of_day;
```

`SUM OVER` без `PARTITION BY` — накопичувальна сума по всьому датасету в порядку `ORDER BY`. З `PARTITION BY` — окрема накопичувальна сума для кожної групи.

`LAG` — доступ до значення попереднього рядка. Корисний для обчислення змін:

```sql
WITH hourly_trips AS (
    SELECT datepart('hour', tpep_pickup_datetime) AS hour_of_day,
           count(*) AS trip_count
    FROM bronze_yellow_trips
    GROUP BY 1
)
SELECT
    hour_of_day,
    trip_count,
    LAG(trip_count) OVER (ORDER BY hour_of_day) AS prev_hour_trips,
    trip_count - LAG(trip_count) OVER (ORDER BY hour_of_day) AS delta
FROM hourly_trips
ORDER BY hour_of_day;
```

### Anti-join через CTE: DQ-паттерн

Класичний прийом для виявлення «осиротілих» значень — рядків, де значення є, але відповідника в довіднику немає:

```sql
WITH pu_zones AS (
    SELECT DISTINCT pu_location_id AS location_id FROM bronze_yellow_trips
),
do_zones AS (
    SELECT DISTINCT do_location_id AS location_id FROM bronze_yellow_trips
)
SELECT p.location_id
FROM pu_zones p
LEFT JOIN do_zones d ON p.location_id = d.location_id
WHERE d.location_id IS NULL
ORDER BY 1;
```

Зони, з яких є посадки, але ніколи немає висадок — або помилка в даних, або зони-виключення (аеропорт). Anti-join виявляє це автоматично.

---

## 4. EXPLAIN ANALYZE: читаємо query plan

### Навіщо знати query plan

Query plan — це відповідь database engine на питання «як саме я виконаю цей запит». Читати query plan — важлива навичка DE: вона дозволяє розуміти, чому запит повільний, і перевіряти, чи engine дійсно застосовує оптимізації (predicate pushdown, правильний join порядок тощо).

`EXPLAIN` показує план без виконання; `EXPLAIN ANALYZE` запускає запит і повертає план з фактичними метриками:

```python
plan = con.sql("EXPLAIN ANALYZE SELECT ...").fetchone()[1]
print(plan)
```

### Три демо з query plan

**E1. Predicate і projection pushdown.** Фільтр і вибір колонок переміщуються прямо у Parquet-сканування. У плані видно `Rows Scanned` — скільки row groups реально прочитано, і список колонок — тільки ті, що в SELECT.

**E2. Локальний файл vs remote URL.** Той самий запит на CloudFront — httpfs робить range-запити, завантажує тільки потрібні row groups. У плані видно HTTP range запити.

**E3. Hash join і build side.** DuckDB будує hash-таблицю з **меншого** датасету. `taxi_zones` (~260 рядків) — build side; `bronze_yellow_trips` (~3 мільйони рядків) — probe side. DuckDB сканує 3 мільйони рядків і перевіряє кожен у hash-таблиці з 260 елементів — набагато ефективніше, ніж навпаки.

Це точний аналог **broadcast join** у Spark: маленький датасет розсилається на всі вузли, великий сканується без переміщення. Повернемося до цього на занятті 15.

---

## 5. DuckDB → Polars: zero-copy через Apache Arrow

DuckDB не має stored procedures за дизайном. Процедурна логіка виноситься назовні, у Python. Перехід відбувається через Apache Arrow — zero-copy:

```python
import polars as pl

# Eager: DuckDB → Polars DataFrame через .pl()
df_pl = con.sql("SELECT pu_location_id, fare_amount FROM bronze_yellow_trips WHERE fare_amount > 20").pl()

# Lazy scan Polars (predicate pushdown видно у плані):
lazy = pl.scan_parquet(str(LOCAL))
filtered = (
    lazy
    .filter(pl.col("fare_amount") > 0)
    .filter(pl.col("passenger_count") > 0)
    .select(["tpep_pickup_datetime", "PULocationID", "fare_amount"])
)
print(filtered.explain(optimized=True))   # predicate pushdown у PARQUET SCAN
```

`.explain(optimized=True)` показує оптимізований план до виконання — корисно для перевірки, що оптимізатор дійсно push-нув фільтр.

---

## 6. COPY: матеріалізуємо Silver у Parquet

`COPY` матеріалізує результат запиту у файл або Hive-партиційовану директорію. Це не те ж саме, що `INSERT INTO` (який пише у DuckDB-таблицю) — `COPY` пише у файлову систему:

```python
# Flat Silver — один Parquet-файл
con.sql("""
COPY (
    SELECT vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime,
           passenger_count, trip_distance, fare_amount, tip_amount, total_amount
    FROM bronze_yellow_trips
)
TO '../../data/lesson-03/silver/yellow_trips.parquet' (FORMAT PARQUET)
""")

# Partitioned Silver — Hive layout по borough
con.sql("""
COPY (
    SELECT t.*, z.Borough AS borough
    FROM bronze_yellow_trips t
    JOIN taxi_zones z ON t.pu_location_id = z.LocationID
)
TO '../../data/lesson-03/silver/partitioned'
(FORMAT PARQUET, PARTITION_BY (borough), OVERWRITE_OR_IGNORE TRUE)
""")
```

`PARTITION_BY` створює ієрархію: `partitioned/borough=Manhattan/`, `borough=Queens/`, тощо. Downstream-запити з `WHERE borough = 'Manhattan'` читають лише відповідну директорію — **partition pruning** в дії.

---

## 7. DuckDB-специфіка, яку варто знати

### CREATE VIEW vs CREATE TABLE

View — збережений SQL-запит. При кожному зверненні до view DuckDB виконує запит заново. Не займає місця на диску. Корисний для логічного розшарування без матеріалізації.

Table — матеріалізовані дані на диску. Запит виконується один раз при створенні. Доступ потім — читання готових даних без повторного обчислення.

Ця ж різниця у dbt: `materialized: view` vs `materialized: table`. Розуміти trade-off — важлива навичка.

### Nested types

DuckDB підтримує `STRUCT`, `LIST`, `MAP` — для JSON-подібних структур без втрати типів:

```sql
SELECT
    {'vendor': vendor_id, 'pickup': pu_location_id} AS trip_struct,
    [fare_amount, tip_amount, total_amount] AS amount_list
FROM bronze_yellow_trips
LIMIT 5;
```

### Prepared statements замість f-string

```python
# Небезпечно — f-string у SQL:
con.execute(f"SELECT * FROM trips WHERE id = {user_input}")

# Правильно — bind-параметр:
con.execute("SELECT * FROM trips WHERE pu_location_id = $1", [161])
```

Ніколи не підставляйте значення через f-string у SQL — це SQL injection. Bind-параметр передає значення окремо від тексту запиту; DuckDB перевіряє тип.

### Single-writer

DuckDB-файл має лише один writer одночасно. Якщо хочете відкрити файл у DBeaver або CLI паралельно з Python-кодом — закрийте з'єднання або підключіться read-only: `duckdb taxi_dwh.duckdb?access_mode=read_only`.

---

## 8. dbt: SQL як код, що складається у DAG

Це центральний новий концепт заняття. Уважно прочитайте цю секцію — вона пояснює, чому dbt став стандартом трансформаційного шару у Modern Data Stack.

### Проблема, яку вирішує dbt

Уявіть: ви написали 40 SQL-запитів для Bronze, Silver і Gold шарів. Кожен вставляє результати в наступну таблицю. Є проблеми:

- **Порядок виконання** — як гарантувати, що Silver виконується після Bronze, а Gold після Silver?
- **Зміни** — якщо змінилась схема Bronze, як дізнатись, які Silver-запити треба оновити?
- **Тестування** — як перевірити, що у Silver немає від'ємних тарифів і немає дублікатів?
- **Документація** — де зберігається опис кожної таблиці і що вона означає?
- **Командна робота** — як два DE можуть одночасно працювати над пов'язаними трансформаціями?

dbt вирішує всі ці проблеми однією парадигмою: **кожен SQL-файл = одна трансформація = один об'єкт у базі**. DDL бере на себе dbt. Залежності між трансформаціями — через `ref()`. Порядок виконання — dbt визначає сам.

### Парадигма: лише SELECT

У dbt ви пишете **тільки `SELECT`**. Не `CREATE TABLE AS`, не `INSERT INTO` — тільки `SELECT`. dbt загортає ваш SELECT у відповідний DDL залежно від обраної materialization:

```sql
-- Ваш файл models/bronze/bronze_yellow_trips.sql:
{{ config(materialized="table") }}

SELECT
    CAST(VendorID AS INTEGER) AS vendor_id,
    CAST(fare_amount AS DECIMAL(10,2)) AS fare_amount,
    ...
FROM {{ source("nyc_tlc", "yellow_trips_raw") }}
```

dbt перетворює це на `CREATE TABLE bronze_yellow_trips AS SELECT ...`. Ім'я файлу стає іменем таблиці. Ви думаєте про трансформацію, а не про DDL.

### Словник dbt

| Сутність | Де лежать дані | Хто веде | Що робить dbt |
|---|---|---|---|
| **source** | Вже в БД або зовні (Parquet, S3) | Upstream-система | Лише посилається (`source()`), ніколи не будує |
| **seed** | CSV-файл у репозиторії | Ви, вручну | Завантажує (`dbt seed`) у таблицю |
| **model** | Обчислюється з інших таблиць | dbt, за вашим SQL | Будує (`dbt run`) |

**`ref()`** — ключовий механізм dbt. Це не просто підстановка імені таблиці. Це **оголошення залежності**:

```sql
-- Файл models/silver/stg_yellow_trips.sql:
SELECT * FROM {{ ref("bronze_yellow_trips") }}
WHERE fare_amount >= 0
```

dbt бачить `ref("bronze_yellow_trips")`, знає, що ця модель залежить від `bronze_yellow_trips`, і гарантує: Bronze виконується перед Silver. Якщо ви виклично напишете ім'я таблиці хардкодом (`FROM bronze_yellow_trips`), dbt не знатиме про залежність і може виконати Silver раніше Bronze.

Аналогія: `ref()` — це `import` у Python. Коли ви пишете `import pandas`, Python знає, що потрібно спочатку завантажити pandas. `ref()` — те саме для SQL-моделей.

**`source()`** — посилання на зовнішні дані, які dbt не будує. Оголошується у `sources.yml`. dbt відстежує їх у lineage як кореневий вузол, але не контролює їх вміст.

**seed** — маленький CSV-довідник, що версіонується разом із кодом у `seeds/`. Класика: `taxi_zone_lookup.csv` (zone ID → borough/zone). Питання-фільтр: «маленька довідкова таблиця, яку ви оновлюєте вручну і хочете тримати в git?» → seed. Самі дані про поїздки (3 мільйони рядків) — це source, не seed.

**test** — SQL-запит, що перевіряє інваріант. Логіка вивернута: тест повертає рядки, які **порушують** правило; 0 рядків = pass. Два види:

- **Schema tests** (декларативні у `schema.yml`): `not_null`, `unique`, `accepted_values`, `relationships` — dbt генерує SQL сам.
- **Singular tests** (довільний `.sql` у `tests/`): наприклад, `SELECT * FROM stg_yellow_trips WHERE fare_amount < 0`.

**materialization** — як dbt зберігає модель:

- `view` — збережений запит, без місця на диску. Виконується щоразу при зверненні. Дешевий шар фільтрації.
- `table` — фізичні дані. Обчислюється один раз при `dbt run`. Для агрегацій і великих таблиць.
- `incremental` — додає тільки нові рядки (advanced, повернемося пізніше).

### Структура проєкту dbt_taxi

```
dbt_taxi/
├── dbt_project.yml          # ім'я проєкту, шляхи, дефолтна materialization, on-run-start hook
├── profiles.yml             # підключення: duckdb, шлях до файлу
├── models/
│   ├── sources.yml          # source('nyc_tlc', 'yellow_trips_raw')
│   ├── bronze/
│   │   └── bronze_yellow_trips.sql   # CAST колонок, materialized: table
│   ├── silver/
│   │   ├── stg_yellow_trips.sql      # DQ-фільтрація через ref(), materialized: view
│   │   └── zone_revenue.sql          # window functions + ref() × 2, materialized: table
│   └── schema.yml           # schema-тести + описи
├── seeds/
│   └── taxi_zone_lookup.csv          # 265 зон (265 рядків)
└── tests/
    └── assert_fare_non_negative.sql  # singular test
```

### Три моделі — той самий SQL, що писали вручну

**Bronze** — типізація через CAST. Той самий результат, що й DDL у секції 2, але без `CHECK` (DQ-фільтрацію перенесли у Silver):

```sql
{{ config(materialized="table") }}
SELECT
    CAST(VendorID AS INTEGER) AS vendor_id,
    CAST(fare_amount AS DECIMAL(10,2)) AS fare_amount,
    ...
FROM {{ source("nyc_tlc", "yellow_trips_raw") }}
```

**Silver** (`stg_yellow_trips`) — дешевий шар фільтрації (materialized: view, щоб не займати місця):

```sql
{{ config(materialized="view") }}
SELECT * FROM {{ ref("bronze_yellow_trips") }}
WHERE fare_amount >= 0
  AND trip_distance >= 0
  AND tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
```

**zone_revenue** — window functions із двома `ref()` — на модель і на seed:

```sql
{{ config(materialized="table") }}
WITH trip_totals AS (
    SELECT
        pu_location_id,
        count(*) AS trip_count,
        sum(fare_amount) AS total_revenue
    FROM {{ ref("stg_yellow_trips") }}
    GROUP BY pu_location_id
),
with_zones AS (
    SELECT t.*, z.Borough AS borough, z.Zone AS zone
    FROM trip_totals t
    JOIN {{ ref("taxi_zone_lookup") }} z ON t.pu_location_id = z.LocationID
)
SELECT *,
    SUM(total_revenue) OVER (PARTITION BY borough ORDER BY pu_location_id) AS cumulative_borough_revenue,
    RANK() OVER (PARTITION BY borough ORDER BY total_revenue DESC) AS revenue_rank_in_borough
FROM with_zones
```

### Тести: де і коли вони виконуються

`schema.yml` — schema-тести: `not_null` на ключових колонках Bronze, `accepted_values` для `payment_type`, `relationships` для перевірки referential integrity.

`tests/assert_fare_non_negative.sql` — singular test. Важлива деталь: він перевіряє `stg_yellow_trips`, а не `bronze_yellow_trips`. Після фільтру у Silver `fare_amount < 0` не може бути — тест завжди зелений. На bronze він був би червоним, бо там є від'ємні значення. Тест прив'язаний до шару, де інваріант *зобов'язаний* виконуватися.

### Запуск і DAG

```bash
dbt debug  --profiles-dir .            # перевірка підключення
dbt seed   --profiles-dir .            # 265 рядків довідника → таблиця
dbt run    --profiles-dir .            # 3 моделі у порядку ref()-залежностей
dbt test   --profiles-dir .            # not_null, accepted_values, relationships, singular
dbt build  --profiles-dir .            # seed + run + test одним DAG
dbt docs generate --profiles-dir . && dbt docs serve   # http://localhost:8080
```

dbt визначає порядок виконання сам за `ref()`: `source → bronze → stg → zone_revenue`. Seed — upstream-вузол для zone_revenue.

У **Lineage Graph** (видно у `dbt docs`) кожен вузол — файл у проєкті. `source`, `seed`, `model` відображаються різними кольорами. Стрілки — залежності через `ref()`.

### Graph operators: точковий запуск

У великих проєктах не завжди треба запускати всі моделі:

```bash
dbt ls --select stg_yellow_trips+          # модель + усе downstream
dbt ls --select +zone_revenue              # усе upstream від zone_revenue
dbt run --select bronze_yellow_trips+      # bronze і все, що від неї залежить
```

`+` перед ім'ям — «всі upstream залежності». `+` після — «вся downstream гілка». Ті самі селектори у `run`, `build`, `test`. Airflow на занятті 08 запускатиме `dbt run --select tag:*` як окремі tasks.

### dbt vs in-house Python: чесне порівняння

| Підхід | In-house Python | dbt |
|---|---|---|
| Трансформація | `INSERT INTO silver SELECT ... FROM bronze` | `SELECT`-модель + `dbt run` |
| Тестування | Ручні assert / pytest | `dbt test` (`not_null`, `unique`, тощо) |
| Документація | Коментарі у коді | `schema.yml` + `dbt docs` |
| Lineage | Не видно | DAG у `dbt docs` |
| Версіонування | git (вручну) | git (нативно: кожна модель = файл) |

Обидва підходи працюють. dbt значно краще у командній роботі і для довгострокового супроводу. Той самий SQL — тепер у файлах, з порядком виконання, тестами і документацією.

---

## Зв'язок із платформою

Заняття запускає managed-шар нашої платформи:

- `data/lesson-03/taxi_dwh.duckdb` — DuckDB-база з типізованою Bronze-таблицею (DQ-рядки відсіяні CHECK) і довідником `taxi_zones`.
- `data/lesson-03/silver/` — Silver: flat `yellow_trips.parquet` і Hive-партиційований `partitioned/borough=*/`.
- `code/dbt_taxi/` — перший dbt-проєкт: source, seed, Bronze/Silver/аналітична моделі, schema- і singular-тести, lineage-граф.

Концепти, що з'являються вперше:
- **schema-on-write** — `CREATE TABLE` з типами і `CHECK`.
- **dbt paradigm** — SQL як код, DAG, `ref()`, `materialization`, `dbt test`.
- **partition pruning** вимірюється на практиці у домашньому завданні.

---

## Ключові терміни

| Термін | Визначення |
|---|---|
| **Schema-on-write** | Схема задається при створенні таблиці (DDL); погані дані відхиляються при INSERT (`NOT NULL`, `CHECK`) |
| **DDL** | Data Definition Language — SQL для визначення структури: `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE` |
| **SUMMARIZE** | DuckDB-команда: статистика (count, null_count, mean, std, min, max, квартилі) по кожній колонці одним запитом |
| **httpfs** | DuckDB-розширення для читання Parquet через HTTP/S3 без завантаження (range-запити) |
| **COPY** | DuckDB-команда: матеріалізація результату запиту у файл або Hive-партиційовану директорію |
| **Hive partitioning** | Ієрархія директорій `column=value/data.parquet`; DuckDB автоматично робить partition pruning |
| **Partition pruning** | При фільтрі по партиційній колонці движок читає лише релевантні директорії |
| **Predicate pushdown** | Фільтр переміщується якнайближче до джерела (скан файлу), щоб зменшити обсяг читання |
| **Window function** | Обчислює значення для кожного рядка з урахуванням сусідніх у «вікні», не колапсуючи результат |
| **QUALIFY** | DuckDB/BigQuery/Snowflake-клауза: фільтр по window function прямо у SELECT, без вкладеного підзапиту |
| **CTE** | Common Table Expression — іменований підзапит у `WITH`-блоці; покращує читабельність |
| **Recursive CTE** | CTE, що посилається сама на себе; для ієрархій і послідовностей |
| **Apache Arrow** | Колонковий in-memory формат — zero-copy міст між DuckDB, Polars і pandas |
| **Hash join build side** | Менший датасет, з якого будується hash-таблиця (probe side — більший); аналог broadcast join у Spark |
| **EXPLAIN / EXPLAIN ANALYZE** | Query plan без виконання / з фактичними метриками виконання |
| **Prepared statement** | SQL із bind-параметрами (`$1`): захист від SQL injection, правильна типізація |
| **dbt** | Фреймворк «SQL as code»: трансформації як моделі (`SELECT`), складені у DAG, з тестуванням і документацією |
| **model** | SQL-файл у `models/`; dbt загортає у `CREATE TABLE/VIEW AS`; ім'я файлу = ім'я об'єкта в БД |
| **source** | Зовнішні дані, на які dbt посилається (`source()`) і відстежує у lineage; не будує |
| **seed** | Маленький CSV-довідник у `seeds/`; dbt завантажує його (`dbt seed`) у таблицю |
| **ref()** | Оголошення залежності між вузлами DAG; dbt визначає порядок виконання з усіх `ref()` |
| **materialization** | Спосіб збереження моделі: `view` (запит без місця на диску), `table` (фізичні дані), `incremental` |
| **dbt test** | Перевірка інваріанта; 0 рядків = pass. Schema test (`not_null`, `unique` тощо) або singular test |
| **lineage / DAG** | Орієнтований граф залежностей моделей; будується з `ref()`/`source()`, видно у `dbt docs` |
| **graph operators** | dbt-селектори (`+model`, `model+`, `--exclude resource_type:test`) для точкового запуску |

---

## Перевір себе

1. У чому різниця між schema-on-read і schema-on-write? Коли доречний кожен підхід у production pipeline?
2. Поясніть власними словами: чому `DECIMAL` краще за `DOUBLE` для фінансових колонок?
3. Що таке window function і чим вона відрізняється від `GROUP BY`? Наведіть приклад задачі, де window function незамінна.
4. Як `ref()` в dbt відрізняється від простого хардкоду імені таблиці? Що відбудеться, якщо написати `FROM bronze_yellow_trips` без `ref()`?
5. Поясніть різницю між `source` і `seed` у dbt. Коли таблиця є source, а коли seed?
6. Чому singular test у dbt перевіряє `stg_yellow_trips`, а не `bronze_yellow_trips`? Що це каже про принцип розміщення тестів?
7. Що таке hash join build side і чому DuckDB обирає менший датасет для побудови hash-таблиці?
8. Поясніть різницю між `materialized: view` і `materialized: table` у dbt. Який trade-off між ними?
