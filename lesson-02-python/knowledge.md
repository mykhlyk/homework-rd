# Заняття 02. Python для Data Engineering

## Навіщо це заняття

Якщо заняття 01 відповідало на питання «що таке DE», то заняття 02 — це перший практичний крок: ми вперше беремо реальний датасет і починаємо з ним щось робити.

Python — де-факто стандарт у Data Engineering. Але «Python» для DE і «Python» для Data Science — це різні речі. Data Scientist переважно пише Python для побудови моделей і аналізу. DE пише Python для *переміщення і трансформації даних*: завантажити файл із мережі, перечитати схему, очистити, перекласти в правильний формат, записати в потрібне місце. Це заняття саме про цей аспект.

Конкретний результат заняття — **landing zone**: перший артефакт silver thread project. Ви завантажуєте NYC TLC Yellow Taxi Trip Records за січень 2024 (близько 3 мільйонів рядків) і Taxi Zone Lookup Table, кладете їх у спільну директорію `data/source/`, і ці файли стають незмінним входом для всіх наступних занять. Це перший з двадцяти кроків побудови вашої Lakehouse-платформи.

Окрім landing zone, заняття дає чотири важливі концепти, які будуть повертатися протягом усього курсу: **schema-on-read** (схема береться з файлу, а не з DDL), **lazy evaluation** (план запиту виконується лише тоді, коли потрібен результат), **контракт даних** (явне оголошення очікувань до структури даних) та **ідемпотентність** (повторний запуск не псує результат).

## Що треба знати заздалегідь

Після заняття 01 у вас встановлено VS Code, Python 3.11+, `uv`, склонований репозиторій. Потрібні базові знання Python: списки, словники, функції, f-рядки, умовні конструкції. Глибоких знань pandas чи API не потрібно — усе пояснюється по ходу.

Запуск коду: `uv run python script.py` або `uv run jupyter lab`. Ніколи `python3` напряму — тільки через `uv`, щоб використовувалось правильне середовище.

---

## 1. Python-екосистема для Data Engineering

### Стек і ролі бібліотек

У занятті використовується наступний набір бібліотек:

| Бібліотека | Роль | Коли використовувати |
|---|---|---|
| `requests` | HTTP-клієнт | Завантаження файлів і REST API |
| `pandas` | DataFrame (eager) | EDA, невеликі датасети, прототипування |
| `polars` | DataFrame (lazy + eager, Rust) | Локальні датасети до десятків GB; замінює pandas там, де важлива швидкість |
| `pyarrow` | Колонковий формат у пам'яті | Читання/запис Parquet, міст між бібліотеками |
| `pandera` | Валідація схеми DataFrame | CI/CD-перевірки на вході pipeline |
| `seaborn` / `matplotlib` | Візуалізація | Швидкий EDA-графік |
| `geopandas` | Геопросторові дані | Spatial join, картографія |

### Чому дві DataFrame-бібліотеки

Питання природне: навіщо вивчати і pandas, і polars? Відповідь проста: pandas — стандарт галузі, ви зустрінете його в кожній команді. Polars — сучасна альтернатива з lazy evaluation і predicate pushdown, яка суттєво швидша на великих датасетах. Заняття показує обидві бібліотеки side-by-side і вимірює різницю прямо в коді, щоб ви могли самостійно обирати залежно від задачі.

DuckDB (SQL поверх файлів) у це заняття навмисно не входить — він відкриває заняття 03.

### Форма коду: notebook-style .py

Код заняття організований у трьох формах: `lesson_02_pct.ipynb` — основний ноутбук, `lesson_02_pct.py` — той самий код у jupytext percent-форматі (зручний для git diff), `lesson_02.py` — плоский скрипт без cell-маркерів.

Важлива ідіома: у ноутбуці ми використовуємо `ic(значення)` (бібліотека icecream) замість `print()` для скалярів, і bare expression (просто ім'я змінної без нічого) для відображення DataFrames. Чому: `ic()` показує і ім'я змінної, і значення, що зручно для debugging; bare expression у Jupyter відображає DataFrame у форматованому вигляді. `print()` у ноутбуці — антипатерн для DE.

---

## 2. Формати файлів

Вибір формату — одне з перших практичних рішень DE. Воно впливає на швидкість читання, стиснення, наявність схеми і сумісність із downstream-системами.

### CSV: зручно для людей, погано для аналітики

CSV — текстовий рядковий формат. Кожен рядок таблиці — один рядок файлу. Переваги: читається будь-яким редактором, підтримується всіма інструментами. Недоліки:

- **Немає типів** — всі значення є рядками до явного парсингу. `"2024-01-15"` — це рядок, а не дата, поки ви не скажете Python парсити.
- **Великий розмір** — числа зберігаються як текст: `12345678` займає 8 байт у тексті, але 4 байти як int32.
- **Повільне читання для аналітики** — щоб порахувати SUM по одній колонці, треба прочитати всі рядки цілком.
- **Схема відсутня** — кожен читач інтерпретує самостійно, що гарантує drift через деякий час.

Коли використовувати CSV: lookup-таблиці (наприклад, `taxi_zone_lookup.csv`), вивантаження для нетехнічних людей, конфігурація.

### JSON / JSONL: для API і потоків подій

JSON — текстовий ієрархічний формат. Нативний формат REST API. JSONL (JSON Lines) або NDJSON — один JSON-об'єкт на рядок; зручний для потокової передачі та логів. Наприклад, GitHub Archive (датасет домашнього завдання) — це gz-стиснений NDJSON.

Недоліки для storage: ще більший розмір ніж CSV; повільний парсинг великих файлів; відсутність суворих типів.

Коли використовувати JSON: відповіді REST API, конфіги, events — але не як storage-формат для великих аналітичних обсягів.

### Parquet: стандарт для аналітики

Apache Parquet — колонковий бінарний формат. Ключова ідея: дані однієї колонки лежать разом на диску.

Порівняйте два підходи:

```
Рядковий (CSV-підхід):
[id=1, name="Alice", fare=12.50, zone=1, date=2024-01-01]
[id=2, name="Bob",   fare=8.00,  zone=2, date=2024-01-01]
[id=3, name="Carol", fare=15.30, zone=3, date=2024-01-01]

Запит: SELECT SUM(fare_amount) FROM trips
→ треба прочитати кожен рядок повністю, хоча потрібна лише колонка fare_amount

Колонковий (Parquet):
[fare: 12.50, 8.00, 15.30, ...]  ← один блок
[zone: 1, 2, 3, ...]
[name: "Alice", "Bob", "Carol", ...]

Той самий запит → читаємо ТІЛЬКИ блок fare_amount. Все інше — навіть не торкаємось.
```

Це **projection pushdown**: читаємо лише потрібні колонки. Для аналітичних запитів (агрегації по 2–3 колонках із таблиць у десятки колонок) різниця в I/O може бути 10–20×.

Додаткові переваги Parquet:
- **Embedded schema** — схема вбудована в файл (у **Parquet footer**). Не потрібно окремо зберігати DDL.
- **Statistics per row group** — у footer зберігається min/max для кожного column chunk. Движок може пропустити цілі row groups без читання, якщо умова фільтрації не може бути виконана.
- **Стиснення** — однорідні дані однієї колонки стискаються набагато краще ніж рядки (LZ4, Snappy, Zstd).
- **Split-able** — кожен row group читається незалежно → паралельна обробка.

Недоліки: не читається людиною; неефективний для покрокового append без переписування.

**Практична демонстрація:** taxi zone lookup приходить як CSV. Зберігаємо в Parquet і порівнюємо розмір — Parquet помітно менший, і при наступних читаннях типи зберігаються явно.

```python
df_zones = pd.read_csv(StringIO(resp.text))
df_zones.to_parquet(ZONE_PARQUET, index=False)

csv_kb = len(resp.content) / 1e3
parquet_kb = ZONE_PARQUET.stat().st_size / 1e3
# Parquet помітно менший
```

### Avro і ORC (коротко)

**Avro** — рядковий бінарний формат зі схемою в заголовку. Стандарт для потокових подій (Kafka, Schema Registry, forward/backward compatibility). Повернемося на заняттях 16–17.

**ORC** — колонковий формат із Hadoop-екосистеми; аналог Parquet, але краще підтримується в Hadoop. У нових проєктах обирайте Parquet.

**Правило**: Parquet — для аналітичного зберігання. Avro — для потокових подій. CSV/JSON — для людей і API.

---

## 3. Завантаження даних через HTTP і REST API

Частина DE-роботи — отримання даних із зовнішніх джерел. Заняття показує два сценарії.

### Socrata REST API і SoQL

NYC Open Data (`data.cityofnewyork.us`) надає дані TLC через **Socrata REST API** із власною мовою запитів — **SoQL** (Socrata Query Language). Це SQL-подібні query-параметри в URL:

```python
params = {
    "$select": "PULocationID,DOLocationID,trip_distance,fare_amount,passenger_count,tpep_pickup_datetime",
    "$where": "trip_distance > 10 AND fare_amount > 0",
    "$order": "fare_amount DESC",
    "$limit": 500,
}
resp = requests.get(f"{SOCRATA_BASE}/{DATASET_ID}.json", params=params, headers=headers, timeout=30)
resp.raise_for_status()
```

Важливий нюанс: Socrata повертає **всі поля як рядки**, навіть числа та дати. Потрібен явний cast:

```python
df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")
df["passenger_count"] = df["passenger_count"].astype("Int64")
df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
```

**Пагінація через `$offset`**: Socrata повертає максимум 1000 рядків за запит. Для більшого обсягу — посторінкова вибірка:

```python
pages = []
for page in range(MAX_PAGES):
    r = requests.get(
        f"{SOCRATA_BASE}/{DATASET_ID}.json",
        params={**page_params, "$limit": PAGE_SIZE, "$offset": page * PAGE_SIZE},
        headers=headers, timeout=30,
    )
    r.raise_for_status()
    batch = r.json()
    if not batch:
        break
    pages.append(pd.DataFrame(batch))
df_paged = pd.concat(pages, ignore_index=True)
```

### Прямий HTTP-download основного датасету

Основний датасет — Parquet з TLC CloudFront (~100 MB). Завантажуємо потоком (`stream=True`) і зберігаємо у landing zone. Запуск **ідемпотентний** — якщо файл вже є, повторно не завантажуємо:

```python
if PARQUET_PATH.exists():
    pass  # вже існує — пропускаємо
else:
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(PARQUET_PATH, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
```

**Основні практики при роботі з HTTP:**
- Завжди вказуйте `timeout=` — без нього запит може висіти вічно.
- `raise_for_status()` — кидає `HTTPError` на 4xx/5xx відповідь.
- Зберігайте raw-відповідь у landing zone, не трансформуйте на льоту.
- Ідемпотентність — повторний запуск не перезаписує і не дублює. Детальніше на занятті 08.

---

## 4. Schema-on-read: читання схеми без завантаження даних

`pq.read_schema()` читає тільки **Parquet footer** — метадані колонок у кінці файлу (кілобайти). Жодного рядка даних у пам'ять. Це найдешевший спосіб перевірити схему перед роботою:

```python
EXPECTED_COLUMNS = {"VendorID", "tpep_pickup_datetime", ..., "Airport_fee"}

actual_schema = pq.read_schema(PARQUET_PATH)
actual_cols = set(actual_schema.names)
missing = EXPECTED_COLUMNS - actual_cols     # порожньо → контракт виконано
```

Це і є **schema-on-read**: схема виводиться з файлу при читанні, а не оголошується через DDL заздалегідь. Зручно для EDA; ризиковано у production — schema drift не викликає помилку, поки ви не почнете читати дані. Schema-on-write (примусове оголошення схеми через `CREATE TABLE`) з'явиться на занятті 03.

Крім `pq.read_schema()`, Polars пропонує `pl.scan_parquet()` — ще один спосіб «заглянути» у файл без завантаження. Але `scan_parquet` будує **план запиту** (LazyFrame), а не читає схему напряму. Деталі — в наступному розділі.

---

## 5. Pandas: eager execution і Arrow-backed режим

### Eager evaluation: читаємо все одразу

`pd.read_parquet()` зчитує **весь файл у пам'ять** одразу. Це **eager evaluation** — операція виконується негайно при виклику.

```python
df = pd.read_parquet(PARQUET_PATH)
ic(df.shape, df.memory_usage(deep=True).sum() / 1e6)
```

За замовчуванням pandas використовує NumPy як backend для зберігання даних. NumPy не підтримує nullable integers, тому колонку з NULL-значеннями pandas кодує як `float64`. Тому `passenger_count` приходить як `float64`, а не `Int8`, навіть якщо семантично це ціле число.

### Arrow-backed pandas: myth-busting

Pandas 2.x підтримує режим `dtype_backend="pyarrow"`, де колонки зберігаються у форматі Apache Arrow замість NumPy.

Поширений міф: «Arrow завжди зменшує пам'ять у pandas». Реальність: **не завжди**. `memory_usage(deep=True)` може показати навіть більше для Arrow-backend через overhead метаданих. Для числових датасетів різниця мінімальна.

```python
df_arrow = pd.read_parquet(PARQUET_PATH, dtype_backend="pyarrow")
ic(mem_default, mem_arrow)   # різниця мінімальна або не на користь Arrow
df_arrow.dtypes.head(6)      # passenger_count: float64 → int64[pyarrow]
```

Але у Arrow-backend є реальні переваги:

**1. Багатша тип-система.** Nullable integers без float-кастингу (`int64[pyarrow]`, а не `float64`), точний `decimal128` для фінансових розрахунків:

```python
# Проблема float64 для грошей (IEEE 754 drift):
0.1 + 0.2                          # → 0.30000000000000004 (!!)
Decimal("0.1") + Decimal("0.2")    # → 0.3  точно
```

Для колонок `fare_amount`, `tip_amount`, `total_amount` правильний тип — `Decimal`/`decimal128`, не `float64`. Помилка в центах може стати помилкою в мільйонах на великих обсягах.

**2. Zero-copy конвертація у Polars.** Polars нативно читає Arrow-буфери. Якщо ваш pandas DataFrame має Arrow-backend, передача в Polars відбувається без копіювання даних:

```python
pl.from_pandas(df)        # numpy-backend → копіювання даних
pl.from_pandas(df_arrow)  # arrow-backend → zero-copy, помітно швидше
```

---

## 6. Polars: lazy evaluation і predicate pushdown

### Ядро і парадигма

Polars написаний на Rust без Python GIL (Global Interpreter Lock) — це означає справжній паралелізм, а не ілюзію. На великих датасетах Polars стабільно швидший за pandas.

Але головне в Polars — не Rust, а **lazy evaluation**.

`pl.scan_parquet()` повертає **LazyFrame** — не дані, а *план запиту*. Дані в пам'ять не читаються:

```python
lazy = pl.scan_parquet(PARQUET_PATH)
import sys
print(sys.getsizeof(lazy))   # дрібна величина — це лише план, не дані
```

Дані матеріалізуються тільки при виклику `.collect()`. До цього моменту ви будуєте граф операцій, а Polars може його оптимізувати.

Ще одна відмінність від pandas: Polars **immutable**. Кожна операція повертає новий об'єкт:

```python
# Pandas — мутує існуючий DataFrame:
df["hour"] = df["tpep_pickup_datetime"].dt.hour

# Polars — повертає новий:
df_with_cols = lf.with_columns([
    pl.col("tpep_pickup_datetime").dt.hour().alias("hour"),
]).collect()
```

### Predicate pushdown: ключова демонстрація

**Predicate pushdown** — це оптимізація, при якій умова фільтрації переміщується якнайближче до джерела даних. У контексті Parquet це означає: row groups, що не задовольняють умову (за статистикою min/max у footer), навіть не читаються з диска.

```python
lazy = pl.scan_parquet(PARQUET_PATH)
filtered_lazy = (
    lazy
    .filter(pl.col("fare_amount") > 0)
    .filter(pl.col("passenger_count") > 0)
    .select(["tpep_pickup_datetime", "PULocationID", "fare_amount", "trip_distance"])
)
print(filtered_lazy.explain(optimized=True))   # видно SELECTION + PROJECT у плані SCAN
df_pl = filtered_lazy.collect()                # тільки тут реальне IO
```

Команда `.explain(optimized=True)` показує оптимізований план запиту ще до виконання. У плані видно, що `SELECTION` (фільтр) і `PROJECT` (вибір колонок) знаходяться безпосередньо всередині `PARQUET SCAN` — до того, як дані потраплять у пам'ять.

Важливе застереження: Polars не гарантує прискорення автоматично. Якщо постійно пере-сканувати датасет з нуля, виграш мінімальний. Справжній виграш — при правильному використанні lazy API з мінімальною кількістю `collect()`.

---

## 7. Benchmark: Pandas vs Polars на реальній задачі

Один і той же запит: `filter(fare_amount > 0)` → `group_by(PULocationID).mean(fare_amount)`.

```python
# Pandas — читає весь файл, потім фільтрує
df_pd = pd.read_parquet(PARQUET_PATH)
result_pd = df_pd[df_pd["fare_amount"] > 0].groupby("PULocationID")["fare_amount"].mean()

# Polars — predicate pushdown під час scan
result_pl = (
    pl.scan_parquet(PARQUET_PATH)
    .filter(pl.col("fare_amount") > 0)
    .group_by("PULocationID")
    .agg(pl.col("fare_amount").mean().alias("avg_fare"))
    .collect()
)
```

На цій задачі Polars помітно швидший — predicate pushdown «показує себе в усій красі».

**Правило великого пальця:** прототипуй у pandas, масштабуй у Polars, важкі SQL-агрегації — у DuckDB або Spark (далі по курсу).

---

## 8. Pandera: валідація схеми як контракт даних

Pandera дозволяє описати очікувану схему DataFrame як Python-клас і автоматично валідувати дані — корисно для CI/CD-перевірок на вході pipeline.

```python
class TaxiTripSchema(pa.DataFrameModel):
    fare_amount: float = pa.Field(ge=-10)       # ge = greater-or-equal; -10 тому що TLC робить корекції
    trip_distance: float = pa.Field(ge=0)
    passenger_count: float = pa.Field(ge=0, le=9, nullable=True)
    PULocationID: int = pa.Field(ge=1, le=265)
    DOLocationID: int = pa.Field(ge=1, le=265)

    class Config:
        coerce = True   # автоматично приводити типи

TaxiTripSchema.validate(df.head(10_000))   # SchemaError при порушенні
```

Зверніть увагу на деталь: `fare_amount` допускає невеликі від'ємні значення (до -10). Це свідоме рішення — TLC робить коригуючі транзакції, і від'ємний тариф може бути легітимним. Якщо б ми поставили `ge=0`, валідація падала б на реальних даних.

Це перший контакт з ідеєю **контракту даних** — явного, машинно-перевірюваного опису того, що очікується від даних. Системно про data contracts — на занятті 14.

---

## 9. EDA на NYC TLC Yellow Taxi Jan 2024

EDA (Exploratory Data Analysis) — попередній аналіз датасету. Мета: зрозуміти структуру, розподіли й аномалії перед тим, як починати будувати pipeline.

### Пошук primary key

Чи є у датасеті природний унікальний ідентифікатор поїздки? Перевіряємо:

```python
unique_combo = df[
    ["tpep_pickup_datetime", "PULocationID", "DOLocationID", "fare_amount"]
].drop_duplicates()
# дублікатів немало → природного primary key немає → surrogate key (рядковий індекс)
```

Висновок: у NYC TLC датасеті немає природного primary key. Це типово для event-based даних. Вирішення — surrogate key (генерований ідентифікатор).

### Аналіз типів

Чи відповідають типи семантиці поля? `PULocationID` — int (очікувано). Але `passenger_count` приходить як `float64` через nullable (краще було б `Int8`). Це наслідок NumPy-backend pandas.

### DQ по п'яти осях

Якість даних (Data Quality) вимірюється за п'ятьма вимірами. У EDA ми лише **фіксуємо** аномалії, не виправляємо — повертатимемось до них на занятті 14.

| Вісь | Що перевіряємо | Код |
|---|---|---|
| **Accuracy** | Від'ємні тарифи; неможлива швидкість | `df[df["fare_amount"] < 0]`; обчислення `speed_mph = trip_distance / duration_h` |
| **Completeness** | Пропущені значення | `df.isnull().sum()` |
| **Consistency** | Час висадки раніше посадки | `df[df["tpep_pickup_datetime"] >= df["tpep_dropoff_datetime"]]` |
| **Timeliness** | Дати поза очікуваним діапазоном | `df["tpep_pickup_datetime"].dt.year != 2024` |
| **Validity** | Значення поза допустимими межами | Нульові пасажири; `PULocationID` поза 1–265 |

Обчислення швидкості як DQ-перевірка:

```python
df["duration_h"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 3600
df["speed_mph"] = df["trip_distance"] / df["duration_h"].replace(0, float("nan"))
fast = df[df["speed_mph"] > 200]   # неможлива швидкість
```

Після EDA студент має зрозуміти: близько 24% записів у цьому датасеті потребують виправлення або видалення. Це не виняток — це норма для реальних production-даних.

### Статистика і візуалізація

`df[[...]].describe()` дає базову статистику. Потім прицільна seaborn-візуалізація з фільтрацією аномалій для читабельності:

```python
clean = df[
    (df["fare_amount"] > 0) & (df["fare_amount"] < 100) &
    (df["trip_distance"] > 0) & (df["trip_distance"] < 50)
]
fig, axes = plt.subplots(1, 2, figsize=(12, 4))
sns.histplot(clean["fare_amount"], bins=60, ax=axes[0])       # пік $10–15, правий хвіст
sns.scatterplot(data=clean.sample(3_000, random_state=42),
                x="trip_distance", y="fare_amount", alpha=0.3, ax=axes[1])
```

---

## 10. Основні операції: Pandas vs Polars side-by-side

Одні й ті самі DE-операції — в обох бібліотеках. Це корисно не тільки для порівняння швидкості, а й для розуміння різних підходів.

### Filter

```python
# Pandas — eager, умова у квадратних дужках
daytime_pd = df[(df["tpep_pickup_datetime"].dt.hour >= 7) & (df["tpep_pickup_datetime"].dt.hour < 19)]

# Polars — lazy, виразний синтаксис
daytime_pl = lf.filter(pl.col("tpep_pickup_datetime").dt.hour().is_between(7, 18)).collect()
```

### Додавання колонки

```python
# Pandas — мутує існуючий DataFrame
df["hour"] = df["tpep_pickup_datetime"].dt.hour

# Polars — immutable, повертає новий
df_with_cols = lf.with_columns([
    pl.col("tpep_pickup_datetime").dt.hour().alias("hour"),
    (pl.col("tpep_pickup_datetime").dt.weekday() >= 5).alias("is_weekend"),
]).collect()
```

### Join

```python
# Pandas
df_joined = df.merge(zones_pd, on="PULocationID", how="left")

# Polars
df_joined = lf.join(zones_pl.lazy(), on="PULocationID", how="left").collect()
```

### Group by

```python
# Pandas
by_hour = df.groupby("hour")["fare_amount"].agg(["mean", "count"]).reset_index().sort_values("hour")

# Polars
by_hour = (
    lf.with_columns(pl.col("tpep_pickup_datetime").dt.hour().alias("hour"))
    .group_by("hour")
    .agg([
        pl.col("fare_amount").mean().alias("mean"),
        pl.col("fare_amount").count().alias("count")
    ])
    .sort("hour")
    .collect()
)
```

### Window function (rolling)

```python
# Pandas
hourly_pd["rolling_3h_avg"] = hourly_pd["fare_amount"].rolling(window=3, min_periods=1).mean()

# Polars
.with_columns(
    pl.col("total_fare").rolling_mean(window_size=3, min_samples=1).alias("rolling_3h_avg")
)
```

### Union

```python
# Pandas
combined = pd.concat([jan_pd, feb_pd], ignore_index=True)

# Polars — вимагає сумісну схему (корисна строгість!)
combined = pl.concat([jan_pl, feb_pl])
```

---

## 11. Геопросторовий вимір: GeoPandas

NYC TLC надає межі таксі-зон у форматі **Shapefile** (ZIP). GeoPandas розширює pandas колонкою `geometry` — кожен рядок має геометрію (точку, лінію, полігон), і підтримує spatial joins.

```python
_r = requests.get(ZONES_ZIP_URL)
with tempfile.TemporaryDirectory() as _tmp:
    with zipfile.ZipFile(io.BytesIO(_r.content)) as _z:
        _z.extractall(_tmp)
    _shp = next(os.path.join(root, f)
                for root, _, files in os.walk(_tmp)
                for f in files if f.endswith(".shp"))
    gdf_zones = gpd.read_file(_shp)

pickup_counts = df.groupby("PULocationID").size().reset_index(name="trip_count")
gdf_with_trips = gdf_zones.merge(pickup_counts, on="location_id", how="left")
gdf_with_trips["trip_count"] = gdf_with_trips["trip_count"].fillna(0)
gdf_with_trips.plot(column="trip_count", cmap="YlOrRd", legend=True)   # хороплет-карта
```

Результат — хороплет-карта Нью-Йорку, де колір зони відповідає кількості поїздок. Manhattan домінує; аеропортні зони (JFK, LGA) добре виділяються.

Ключові GeoPandas-операції: `.sjoin()` (просторовий join), `.to_crs()` (перепроєкція систем координат), `.buffer()`, `.dissolve()`.

---

## Зв'язок із проєктом

Заняття закладає **landing zone** — незмінний вхід для всього курсу.

Що будується:
- `data/source/yellow_tripdata_2024-01.parquet` — основний датасет у shared source dir.
- `data/source/taxi_zone_lookup.parquet` — довідник зон (265 зон).
- Артефакти EDA (`eda_plots.png`, `nyc_taxi_heatmap.png`) у `data/lesson-02/`.

Шляхи даних: код запускається з директорії `code/`; усі шляхи відносні. Спільне джерело — `../../data/source/`. Дані не комітяться до репозиторію.

Концепти, що вперше з'являються тут:
- **Schema-on-read** через Parquet footer і Polars lazy scan (schema-on-write — заняття 03, schema-managed — заняття 10).
- **Lazy vs eager execution** і **predicate / projection pushdown**.
- **Контракт даних** — першочерговий контакт через Pandera і `pq.read_schema`.
- **Ідемпотентність** — landing-файли незмінні; повторний запуск не перезаписує. Детально — заняття 08.
- **DQ-issues** — від'ємні тарифи, нулі, аномалії швидкості. Фіксуємо тут, виправляємо на занятті 14.

---

## Ключові терміни

| Термін | Визначення |
|---|---|
| **EDA (Exploratory Data Analysis)** | Попередній аналіз датасету для розуміння структури, розподілів і аномалій |
| **Schema-on-read** | Схема виводиться в момент читання файлу, не при записі; немає примусового контракту |
| **Parquet** | Колонковий бінарний файловий формат зі вбудованою схемою і стисненням; стандарт для аналітики |
| **Parquet footer** | Метадані файлу (схема, statistics per row group у вигляді min/max) у кінці Parquet-файлу; `pq.read_schema()` читає лише його |
| **Eager execution** | Операція виконується негайно, результат матеріалізується в RAM (Pandas) |
| **Lazy execution** | Будується граф операцій без виконання; реальне IO — лише при `.collect()` (Polars LazyFrame) |
| **LazyFrame** | Polars-об'єкт, що представляє невиконаний план запиту; у пам'яті — тільки план, не дані |
| **Predicate pushdown** | Оптимізація: умова фільтрації застосовується на рівні сканування файлу, до завантаження в пам'ять |
| **Projection pushdown** | Оптимізація: з файлу читаються лише потрібні колонки |
| **Arrow-backed Pandas** | Pandas 2.x режим `dtype_backend="pyarrow"` — колонки у форматі Apache Arrow; багатша тип-система, zero-copy у Polars |
| **Apache Arrow** | Колонковий формат у пам'яті (in-memory); спільна основа Pandas (arrow-backend), Polars і Parquet IO |
| **SoQL** | Socrata Query Language — SQL-подібна мова для NYC Open Data API (`$select`, `$where`, `$limit`, `$offset`) |
| **Pandera** | Бібліотека валідації DataFrame: схема як Python-клас, перевірка типів і діапазонів; контракт даних |
| **DQ dimensions** | П'ять вимірів якості даних: Accuracy, Completeness, Consistency, Timeliness, Validity |
| **Idempotency** | Властивість операції: повторний запуск з тими самими вхідними даними дає той самий результат без побічних ефектів |
| **Landing zone** | Шар незмінних raw-файлів (Parquet, CSV, NDJSON); вхід для всього курсу |
| **Medallion architecture** | Шари даних landing → bronze → silver → gold зі зростанням чистоти й бізнес-цінності |
| **GeoPandas** | Розширення Pandas для геопросторових даних: колонка `geometry`, `.sjoin()`, `.to_crs()` |
| **Zero-copy** | Передача даних між бібліотеками через спільний буфер пам'яті без копіювання; можлива завдяки Apache Arrow |

---

## Перевір себе

1. Чому Parquet є кращим форматом для аналітики, ніж CSV? Назвіть принаймні три конкретні причини.
2. Поясніть різницю між eager і lazy execution на прикладі pandas і Polars. Коли lazy дає виграш у продуктивності?
3. Що таке predicate pushdown і як він пов'язаний зі структурою Parquet-файлу (row groups, footer, statistics)?
4. Чому Arrow-backed pandas `dtype_backend="pyarrow"` не завжди зменшує обсяг пам'яті? Які є реальні переваги Arrow-backend?
5. Чому `fare_amount` зберігається як `DECIMAL`, а не `float64`? Що таке IEEE 754 drift і як він впливає на фінансові розрахунки?
6. Поясніть концепцію ідемпотентності власними словами. Як вона реалізована в landing zone цього заняття?
7. Що таке schema-on-read? Чому це зручно для EDA і чому ризиковано у production pipeline?
8. Яку DQ-проблему виявляє перевірка швидкості поїздки (`speed_mph > 200`)? Що це говорить про якість вхідних даних?
