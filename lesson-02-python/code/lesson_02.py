import io
import os
import sys
import tempfile
import time
import zipfile
from io import StringIO
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import pandera as pa
import polars as pl
import pyarrow.parquet as pq
import requests
import seaborn as sns

YEAR, MONTH = 2024, 1
# Shared source dir (one copy for every lesson) and this lesson's output dir.
# Run from this code/ dir; paths are relative to it.
SOURCE_DIR = Path("../../data/source")
OUT_DIR = Path("../../data/lesson-02")
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONES_ZIP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

PARQUET_PATH = SOURCE_DIR / f"yellow_tripdata_{YEAR}-{MONTH:02d}.parquet"
ZONE_PARQUET = SOURCE_DIR / "taxi_zone_lookup.parquet"

SOURCE_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

"""
# Заняття 02 — Python для Data Engineering

**Датасет:** NYC TLC Yellow Taxi Trip Records, January 2024
**Формат:** Parquet (~100 MB, ~3 млн рядків)
**Довідник зон:** NYC TLC Taxi Zone Lookup (265 рядків)

Структура ноутбука:
1. Завантаження через REST API — zone lookup як приклад HTTP → DataFrame → Parquet
2. Завантаження основного датасету (ідемпотентно)
3. Інспекція схеми без завантаження даних (PyArrow footer)
4. Pandas: eager читання, Arrow-backed режим
5. Polars: lazy evaluation, predicate pushdown
6. Benchmark: Pandas vs Polars
7. Pandera: валідація схеми
8. EDA: primary key, типи даних, якість даних по 5 осях, статистика, візуалізація
9. Основні операції: filter, add column, join, group by, window function, union
10. Геопросторовий вимір
"""

# ── 1. Завантаження через REST API ──────────────────────────────────────────────────

"""
## 1. Завантаження через REST API

Демонстрація роботи з HTTP API за допомогою `requests`.

**Джерело:** NYC TLC — Taxi Zone Lookup CSV
**Аутентифікація:** не потрібна (публічний endpoint)

Типовий flow отримання даних через REST API:
1. `GET` запит
2. Парсинг тіла відповіді
3. Збереження у внутрішній формат
"""

"""
### GET-запит
"""

resp = requests.get(ZONE_CSV_URL, timeout=30)
resp.raise_for_status()

print(f"HTTP status:    {resp.status_code}")
print(f"Content-Type:   {resp.headers.get('Content-Type')}")
print(f"Розмір тіла:    {len(resp.content) / 1e3:.1f} KB")

"""
### Парсинг CSV із рядка відповіді

`StringIO` дозволяє передати рядок туди, де очікується файл.
Жодного файлу на диску на цьому кроці не з'являється.
"""

df_zones = pd.read_csv(StringIO(resp.text))

print(f"\nЗавантажено {len(df_zones)} зон")
print(df_zones.head(10))
print(f"\nBorough distribution:\n{df_zones['Borough'].value_counts()}")

"""
### Збереження у Parquet

Зберігаємо Parquet замість CSV:
- менший розмір завдяки колонковому стисненню
- типи зберігаються явно (не потрібен зайвий cast при читанні)
- predicate pushdown у downstream запитах
"""

df_zones.to_parquet(ZONE_PARQUET, index=False)

csv_kb = len(resp.content) / 1e3
parquet_kb = ZONE_PARQUET.stat().st_size / 1e3
print(f"\nCSV:     {csv_kb:.1f} KB")
print(f"Parquet: {parquet_kb:.1f} KB  ({csv_kb / parquet_kb:.1f}x менший)")

# ── 2. Завантаження основного датасету ────────────────────────────────────────

"""
## 2. Завантаження основного датасету

NYC TLC Yellow Taxi Trip Records за January 2024.
Файл: `data/landing/yellow_tripdata_2024-01.parquet` (~100 MB, ~3 млн рядків)

Скрипт **ідемпотентний** — повторний запуск нічого не перезаписує.
"""

if PARQUET_PATH.exists():
    print(f"Вже існує: {PARQUET_PATH}  ({PARQUET_PATH.stat().st_size / 1e6:.1f} MB)")
else:
    url = f"{BASE_URL}/{PARQUET_PATH.name}"
    print(f"Завантажуємо {url} ...")
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(PARQUET_PATH, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Збережено: {PARQUET_PATH}  ({PARQUET_PATH.stat().st_size / 1e6:.1f} MB)")

# ── 3. Інспекція схеми (без завантаження даних) ───────────────────────────────

"""
## 3. Інспекція схеми без завантаження даних

`pq.read_schema` читає тільки **footer** Parquet-файлу — метадані колонок,
але не самі рядки. Дешева перевірка перед початком роботи.
"""

EXPECTED_COLUMNS = {
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID",
    "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "tip_amount", "total_amount",
    "congestion_surcharge", "Airport_fee",
}

actual_schema = pq.read_schema(PARQUET_PATH)
actual_cols = set(actual_schema.names)
missing = EXPECTED_COLUMNS - actual_cols

print(f"Колонок у файлі: {len(actual_cols)}")
print(f"Відсутні:        {missing if missing else 'немає'}")
print(f"\nПовна схема:\n{actual_schema}")

# ── 4. Pandas: читання Parquet ────────────────────────────────────────────────

"""
## 4. Pandas: читання Parquet

`pd.read_parquet` зчитує весь файл у пам'ять (**eager evaluation**).
За замовчуванням використовує **PyArrow** як engine — Apache Arrow columnar format.
"""

df = pd.read_parquet(PARQUET_PATH)
print(f"Shape:  {df.shape}")
print(f"Memory: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")
print(f"\nDtypes:\n{df.dtypes}")

"""
### Arrow-backed pandas (pandas 2.x)

`dtype_backend="pyarrow"` зберігає колонки як **Arrow arrays** всередині pandas.

Поширений міф: «Arrow завжди зменшує пам'ять у pandas». Насправді — **не завжди**.
`memory_usage(deep=True)` може показати більше для Arrow-backend через Arrow-metadata overhead.
Виграш залежить від типів і розподілу даних; для числових датасетів різниця мінімальна.

Реальні переваги Arrow-backend у pandas:

1. **Багатший тип-системи**: nullable integers без float-кастингу (`int64[pyarrow]`, не `float64`),
   точний `decimal128` для фінансових розрахунків, нативні `date32` / `time32`.
2. **Інтероперабельність**: Polars нативно читає Arrow-буфери — конвертація zero-copy,
   без зайнього копіювання даних у пам'яті.
"""

df_arrow = pd.read_parquet(PARQUET_PATH, dtype_backend="pyarrow")
mem_default = df.memory_usage(deep=True).sum() / 1e6
mem_arrow = df_arrow.memory_usage(deep=True).sum() / 1e6

print(f"Default (numpy-backed): {mem_default:.1f} MB")
print(f"Arrow-backed:           {mem_arrow:.1f} MB")
print("(різниця може бути мінімальною або на користь numpy)")
print(f"\nArrow dtypes (порівняйте passenger_count: float64 → int64[pyarrow]):")
print(df_arrow.dtypes.head(6))

"""
### Тип-система Arrow: nullable integers і точний Decimal

Numpy не підтримує nullable integers — pandas кодує їх як `float64`.
Arrow має нативний nullable int і точний `decimal128`.
"""

print(f"\nnumpy-backend passenger_count: {df['passenger_count'].dtype}")
print(f"arrow-backend passenger_count: {df_arrow['passenger_count'].dtype}")

import decimal
d1, d2 = decimal.Decimal("0.1"), decimal.Decimal("0.2")
print(f"\nFloat:   0.1 + 0.2 = {0.1 + 0.2}")
print(f"Decimal: 0.1 + 0.2 = {d1 + d2}  ← точно (важливо для фінансових розрахунків)")

# ── 5. Polars: lazy evaluation ────────────────────────────────────────────────

"""
## 5. Polars: lazy evaluation

### Arrow → Polars: zero-copy конвертація

Polars нативно читає Arrow-буфери. З numpy-backend pandas змушений копіювати дані;
з Arrow-backend — ні. Це практичний сценарій, де Arrow-backend у pandas виправданий.
"""

t0 = time.perf_counter()
_pl_from_numpy = pl.from_pandas(df)
t_numpy_conv = time.perf_counter() - t0

t0 = time.perf_counter()
_pl_from_arrow = pl.from_pandas(df_arrow)
t_arrow_conv = time.perf_counter() - t0

print(f"pandas (numpy) → Polars: {t_numpy_conv:.3f}s")
print(f"pandas (arrow) → Polars: {t_arrow_conv:.3f}s")
print(f"Arrow-конвертація швидша у {t_numpy_conv / t_arrow_conv:.1f}x")

"""
### Lazy evaluation

`pl.scan_parquet` **не читає дані** — будує LazyFrame (план запиту).
Дані матеріалізуються тільки при `.collect()`.

Ключова перевага: **predicate pushdown** — фільтри застосовуються
під час читання файлу, не завантажуючи зайві рядки в пам'ять.
"""

lazy = pl.scan_parquet(PARQUET_PATH)
print(f"LazyFrame у пам'яті (тільки план): {sys.getsizeof(lazy)} bytes")

filtered_lazy = (
    lazy
    .filter(pl.col("fare_amount") > 0)
    .filter(pl.col("passenger_count") > 0)
    .select(["tpep_pickup_datetime", "PULocationID", "fare_amount", "trip_distance"])
)

print("\nОптимізований план запиту (видно predicate pushdown):")
print(filtered_lazy.explain(optimized=True))

df_pl = filtered_lazy.collect()
print(f"\nПісля .collect(): {df_pl.shape}  ({sys.getsizeof(df_pl) / 1e6:.1f} MB)")
print(df_pl.head(3))

# ── 6. Benchmark: Pandas vs Polars ────────────────────────────────────────────

"""
## 6. Benchmark: Pandas vs Polars

Та сама задача для обох бібліотек:
- `filter(fare_amount > 0)`
- `groupby(PULocationID).mean(fare_amount)`

Polars використовує **predicate pushdown** — фільтрує під час читання файлу,
не завантажуючи зайві рядки в пам'ять.
"""

t0 = time.perf_counter()
df_pd = pd.read_parquet(PARQUET_PATH)
result_pd = df_pd[df_pd["fare_amount"] > 0].groupby("PULocationID")["fare_amount"].mean()
t_pandas = time.perf_counter() - t0

t0 = time.perf_counter()
result_pl = (
    pl.scan_parquet(PARQUET_PATH)
    .filter(pl.col("fare_amount") > 0)
    .group_by("PULocationID")
    .agg(pl.col("fare_amount").mean().alias("avg_fare"))
    .collect()
)
t_polars = time.perf_counter() - t0

print(f"\n{'Бібліотека':<12}  {'Час':>8}  {'Зон':>6}")
print("-" * 32)
print(f"{'Pandas':<12}  {t_pandas:>6.2f}s  {len(result_pd):>6}")
print(f"{'Polars':<12}  {t_polars:>6.2f}s  {len(result_pl):>6}")
print(f"\nPolars швидший у {t_pandas / t_polars:.1f}x")

# ── 7. Pandera: валідація схеми ───────────────────────────────────────────────

"""
## 7. Pandera: валідація схеми

Pandera описує очікувану схему як Python-клас і валідує DataFrame.
Корисно для CI/CD перевірок на вході пайплайну.

`pa.Field(ge=0)` — greater or equal. `nullable=True` — nullable column.
"""


class TaxiTripSchema(pa.DataFrameModel):
    fare_amount: float = pa.Field(ge=-10)       # негативні значення можливі (корекції TLC)
    trip_distance: float = pa.Field(ge=0)
    passenger_count: float = pa.Field(ge=0, le=9, nullable=True)
    PULocationID: int = pa.Field(ge=1, le=265)
    DOLocationID: int = pa.Field(ge=1, le=265)

    class Config:
        coerce = True


try:
    TaxiTripSchema.validate(df.head(10_000))
    print("Schema OK (validated on 10 000 rows sample)")
except pa.errors.SchemaError as e:
    print(f"Schema violation:\n{e}")

# ── 8. EDA ────────────────────────────────────────────────────────────────────

"""
## 8. EDA: пошук первинного ключа

Чи є природний унікальний ідентифікатор поїздки?
"""

total = len(df)
unique_combo = df[
    ["tpep_pickup_datetime", "PULocationID", "DOLocationID", "fare_amount"]
].drop_duplicates()

print(f"Всього рядків:                 {total:>10,}")
print(f"Унікальних (dt, PU, DO, fare): {len(unique_combo):>7,}")
print(f"Дублікатів:                    {total - len(unique_combo):>10,}")
print("\nВисновок: природного primary key немає — використовуємо surrogate key (рядковий індекс).")

"""
### Аналіз типів даних

Перевіряємо типи колонок — чи відповідають вони семантиці поля?
"""

print(df.dtypes)
print(f"\nPULocationID:    {df['PULocationID'].dtype} — очікуємо int, маємо int ✓")
print(f"fare_amount:     {df['fare_amount'].dtype} — float64, підходить для суми в USD")
print(f"passenger_count: {df['passenger_count'].dtype} — float64 через nullable, краще було б Int8")

"""
### DQ по п'яти осях

#### Accuracy — чи значення відповідають реальності?
"""

neg_fares = df[df["fare_amount"] < 0]
print(f"Негативні тарифи (fare_amount < 0): {len(neg_fares):,}")

df["duration_h"] = (
    df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
).dt.total_seconds() / 3600
df["speed_mph"] = df["trip_distance"] / df["duration_h"].replace(0, float("nan"))
fast = df[df["speed_mph"] > 200]
print(f"Неможлива швидкість (> 200 mph):    {len(fast):,}")

"""
#### Completeness — відсутні значення
"""

nulls = df.isnull().sum()
print(nulls[nulls > 0].to_string())

"""
#### Consistency — внутрішня узгодженість
"""

time_anomaly = df[df["tpep_pickup_datetime"] >= df["tpep_dropoff_datetime"]]
print(f"Pickup >= dropoff: {len(time_anomaly):,}")

"""
#### Timeliness — чи дати в очікуваному діапазоні?
"""

print(f"Діапазон pickup: {df['tpep_pickup_datetime'].min()} → {df['tpep_pickup_datetime'].max()}")
out_of_range = df[df["tpep_pickup_datetime"].dt.year != 2024]
print(f"Рядки не 2024 року: {len(out_of_range):,}")

"""
#### Validity — значення в допустимих межах?
"""

zero_pax = df[df["passenger_count"].isin([0]) | df["passenger_count"].isna()]
print(f"Zero/null passengers: {len(zero_pax):,}")

invalid_pu = df[(df["PULocationID"] > 265) | (df["PULocationID"] < 1)]
print(f"Invalid PULocationID: {len(invalid_pu):,}")

"""
### Статистичний аналіз
"""

print(df[["fare_amount", "trip_distance", "passenger_count", "duration_h"]].describe())

"""
### Візуалізація (seaborn)

Фільтруємо очевидні аномалії для кращої читаності графіків.
"""

clean = df[
    (df["fare_amount"] > 0) & (df["fare_amount"] < 100) &
    (df["trip_distance"] > 0) & (df["trip_distance"] < 50)
]

fig, axes = plt.subplots(1, 2, figsize=(12, 4))

sns.histplot(clean["fare_amount"], bins=60, ax=axes[0])
axes[0].set_title("Розподіл fare_amount")
axes[0].set_xlabel("USD")

sns.scatterplot(
    data=clean.sample(3_000, random_state=42),
    x="trip_distance",
    y="fare_amount",
    alpha=0.3,
    ax=axes[1],
)
axes[1].set_title("fare_amount vs trip_distance")

plt.tight_layout()
plt.savefig(OUT_DIR / "eda_plots.png", dpi=100)
plt.show()
print(f"Збережено: {OUT_DIR / 'eda_plots.png'}")

# ── 9. Основні операції ───────────────────────────────────────────────────────

"""
## 9. Основні операції: Pandas vs Polars

Кожну операцію показуємо в обох бібліотеках.
Pandas — eager, знайомий синтаксис.
Polars — lazy за замовчуванням, виразний API, швидший на великих даних.
"""

lf = pl.scan_parquet(PARQUET_PATH)

"""
### Filter

Залишаємо тільки денні поїздки (7:00–18:59).
"""

# Pandas
daytime_pd = df[
    (df["tpep_pickup_datetime"].dt.hour >= 7) &
    (df["tpep_pickup_datetime"].dt.hour < 19)
]
print(f"Pandas — денні поїздки: {len(daytime_pd):,}")

# Polars
daytime_pl = (
    lf.filter(pl.col("tpep_pickup_datetime").dt.hour().is_between(7, 18))
    .collect()
)
print(f"Polars — денні поїздки: {len(daytime_pl):,}")

"""
### Add column

Додаємо `hour` і `is_weekend` на основі часу відправлення.
"""

# Pandas
df["hour"] = df["tpep_pickup_datetime"].dt.hour
df["is_weekend"] = df["tpep_pickup_datetime"].dt.dayofweek >= 5
print(df[["tpep_pickup_datetime", "hour", "is_weekend"]].head(3))

# Polars
df_with_cols = (
    lf.with_columns([
        pl.col("tpep_pickup_datetime").dt.hour().alias("hour"),
        (pl.col("tpep_pickup_datetime").dt.weekday() >= 5).alias("is_weekend"),
    ])
    .collect()
)
print(df_with_cols[["tpep_pickup_datetime", "hour", "is_weekend"]].head(3))

"""
### Join

Додаємо назву зони відправлення (`PU_Zone`) з довідника.
Довідник уже в Parquet — зберегли його на кроці 1 (отримання через REST API).
"""

# Pandas
zones_pd = pd.read_parquet(ZONE_PARQUET).rename(
    columns={"LocationID": "PULocationID", "Zone": "PU_Zone"}
)[["PULocationID", "PU_Zone"]]
df_joined_pd = df.merge(zones_pd, on="PULocationID", how="left")
print(f"Pandas — top zones:\n{df_joined_pd['PU_Zone'].value_counts().head(5)}")

# Polars
zones_pl = pl.read_parquet(ZONE_PARQUET).rename(
    {"LocationID": "PULocationID", "Zone": "PU_Zone"}
).select(["PULocationID", "PU_Zone"])
df_joined_pl = lf.join(zones_pl.lazy(), on="PULocationID", how="left").collect()
print(f"\nPolars — top zones:\n{df_joined_pl['PU_Zone'].value_counts().head(5)}")

"""
### Group by

Середній тариф і кількість поїздок по годині доби.
"""

# Pandas
by_hour_pd = (
    df.groupby("hour")["fare_amount"]
    .agg(["mean", "count"])
    .reset_index()
    .sort_values("hour")
)
print(f"Pandas:\n{by_hour_pd.head(6)}")

# Polars
by_hour_pl = (
    lf.with_columns(pl.col("tpep_pickup_datetime").dt.hour().alias("hour"))
    .group_by("hour")
    .agg([
        pl.col("fare_amount").mean().alias("mean"),
        pl.col("fare_amount").count().alias("count"),
    ])
    .sort("hour")
    .collect()
)
print(f"\nPolars:\n{by_hour_pl.head(6)}")

"""
### Window function

Rolling 3-годинне середнє по погодинній виручці.
"""

# Pandas
hourly_pd = df.groupby("hour")["fare_amount"].sum().reset_index().sort_values("hour")
hourly_pd["rolling_3h_avg"] = hourly_pd["fare_amount"].rolling(window=3, min_periods=1).mean()
print(f"Pandas:\n{hourly_pd}")

# Polars
hourly_pl = (
    lf.with_columns(pl.col("tpep_pickup_datetime").dt.hour().alias("hour"))
    .group_by("hour")
    .agg(pl.col("fare_amount").sum().alias("total_fare"))
    .sort("hour")
    .with_columns(
        pl.col("total_fare").rolling_mean(window_size=3, min_periods=1).alias("rolling_3h_avg")
    )
    .collect()
)
print(f"\nPolars:\n{hourly_pl}")

"""
### Union (concat)

Симулюємо об'єднання двох місяців — зсуваємо Jan на ~1 місяць для демо.
"""

# Pandas
jan_pd = pd.read_parquet(PARQUET_PATH)
feb_pd = jan_pd.sample(100_000, random_state=42).copy()
feb_pd["tpep_pickup_datetime"] = feb_pd["tpep_pickup_datetime"] + pd.DateOffset(months=1)
feb_pd["tpep_dropoff_datetime"] = feb_pd["tpep_dropoff_datetime"] + pd.DateOffset(months=1)
combined_pd = pd.concat([jan_pd, feb_pd], ignore_index=True)
print(f"Pandas — Jan: {len(jan_pd):,} + Feb: {len(feb_pd):,} = {len(combined_pd):,}")

# Polars
jan_pl = pl.read_parquet(PARQUET_PATH)
feb_pl = jan_pl.sample(100_000, seed=42).with_columns([
    pl.col("tpep_pickup_datetime").dt.offset_by("1mo"),
    pl.col("tpep_dropoff_datetime").dt.offset_by("1mo"),
])
combined_pl = pl.concat([jan_pl, feb_pl])
print(f"Polars — Jan: {len(jan_pl):,} + Feb: {len(feb_pl):,} = {len(combined_pl):,}")

# ── 10. Геопросторовий вимір ──────────────────────────────────────────────────

"""
## 10. Геопросторовий вимір (GeoPandas)

NYC TLC надає межі таксі-зон у форматі Shapefile (ZIP).
GeoPandas дозволяє робити **spatial joins** — та сама логіка, що й у звичайних DataFrame.

Завантажуємо Shapefile з TLC CloudFront,
об'єднуємо з кількістю поїздок і будуємо хороплет-карту.
"""

_r = requests.get(ZONES_ZIP_URL)
with tempfile.TemporaryDirectory() as _tmp:
    with zipfile.ZipFile(io.BytesIO(_r.content)) as _z:
        _z.extractall(_tmp)
    _shp = next(
        os.path.join(root, f)
        for root, _, files in os.walk(_tmp)
        for f in files if f.endswith(".shp")
    )
    gdf_zones = gpd.read_file(_shp)
gdf_zones = gdf_zones.rename(columns={"LocationID": "location_id"})
print(f"GeoDataFrame: {gdf_zones.shape}")
print(gdf_zones[["location_id", "zone", "borough", "geometry"]].head(3))

pickup_counts = (
    df.groupby("PULocationID")
    .size()
    .reset_index(name="trip_count")
    .assign(location_id=lambda x: x["PULocationID"].astype(str))
)
gdf_zones["location_id"] = gdf_zones["location_id"].astype(str)
gdf_with_trips = gdf_zones.merge(pickup_counts, on="location_id", how="left")
gdf_with_trips["trip_count"] = gdf_with_trips["trip_count"].fillna(0)

fig, ax = plt.subplots(figsize=(10, 8))
gdf_with_trips.plot(column="trip_count", cmap="YlOrRd", legend=True, ax=ax)
ax.set_title("NYC Taxi — кількість поїздок по зонах (Jan 2024)")
ax.set_axis_off()
plt.tight_layout()
plt.savefig(OUT_DIR / "nyc_taxi_heatmap.png", dpi=100)
plt.show()
print(f"Збережено: {OUT_DIR / 'nyc_taxi_heatmap.png'}")
