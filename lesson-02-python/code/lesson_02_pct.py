# %% [markdown]
# # Заняття 02 — Python для Data Engineering
#
# ## Основні цілі заняття:
# * Показати як працювати з різними джерелами даних (REST-api, file-based).
# * Поглянути на основні бібліотеки для **локальної** роботи з даними в Python (`pandas` і `polars`) і побачити основну різницю між ними (парадигма, синтаксис, performance).
# * Швидко поглянути на Data Quality в локальній роботі з даними
# * Провести __exploratory data analysis__ основного датасету та загалом познайомитися з ним

# %% [markdown]
# * **Датасет:** NYC TLC Yellow Taxi Trip Records, January 2024
# * **Формат:** Parquet (~100 MB, ~3 млн рядків)
# * **Довідник зон:** NYC TLC Taxi Zone Lookup (265 рядків)

# %% [markdown]
# Структура ноутбука:
# 1. Отримання даних через REST API — Socrata NYC Open Data: JSON, SoQL, пагінація, join із zone lookup
# 2. Завантаження основного датасету
# 3. Інспекція схеми без завантаження даних (PyArrow footer)
# 4. Pandas: eager читання, Arrow-backed режим
# 5. Polars: lazy evaluation, predicate pushdown
# 6. Benchmark: Pandas vs Polars
# 7. Pandera: валідація схеми
# 8. EDA: primary key, типи даних, якість даних по 5 осях, статистика, візуалізація
# 9. Основні операції: filter, add column, join, group by, window function, union
# 10. Геопросторовий вимір

# %% [markdown]
# ## 1. Отримання даних через REST API (Socrata / NYC Open Data)
#
# NYC Open Data надає TLC taxi-дані через **Socrata REST API**.
# Той самий домен — `PULocationID` / `DOLocationID` — але доступ через HTTP JSON.
#
# Типовий REST API flow:
# 1. `GET` запит з query-параметрами
# 2. Парсинг JSON-відповіді
# 3. Пагінація через `$offset`
# 4. Збереження у внутрішній формат (Parquet)
#
# **SoQL** (Socrata Query Language) — SQL-подібні параметри в URL:
# `$where`, `$select`, `$order`, `$limit`, `$offset`
#
# App token (опціонально, збільшує rate limit):
# https://data.cityofnewyork.us/profile/app_tokens

# %%
import os
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

# Shared source dir (one copy for every lesson) and this lesson's output dir.
# Run from this code/ dir; paths are relative to it.
SOURCE_DIR = Path("../../data/source")
OUT_DIR = Path("../../data/lesson-02")
SOURCE_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Yellow Taxi Trip Data 2019 on NYC Open Data
# ⚠️ Verify dataset ID at: https://data.cityofnewyork.us (search "Yellow Taxi Trip Data")
SOCRATA_BASE = "https://data.cityofnewyork.us/resource"
DATASET_ID = "2upf-qytp"

APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}

# %% [markdown]
# ### GET-запит з SoQL параметрами

# %%
params = {
    "$select": "PULocationID,DOLocationID,trip_distance,fare_amount,passenger_count,tpep_pickup_datetime",
    "$where": "trip_distance > 10 AND fare_amount > 0",
    "$order": "fare_amount DESC",
    "$limit": 500,
}

resp = requests.get(
    f"{SOCRATA_BASE}/{DATASET_ID}.json",
    params=params,
    headers=headers,
    timeout=30,
)
resp.raise_for_status()

# %%
from icecream import ic
ic(resp.status_code, resp.headers.get("Content-Type"), len(resp.json()))

# %% [markdown]
# Тіло відповіді — список JSON-об'єктів (перші два):

# %%
resp.json()

# %% [markdown]
# ### Парсинг у DataFrame
#
# Socrata повертає всі поля як рядки — потрібен явний cast числових колонок.

# %%
df_api = pd.DataFrame(resp.json())

for col in ["PULocationID", "DOLocationID", "passenger_count"]:
    df_api[col] = pd.to_numeric(df_api[col], errors="coerce").astype("Int64")
for col in ["trip_distance", "fare_amount"]:
    df_api[col] = pd.to_numeric(df_api[col], errors="coerce")
df_api["tpep_pickup_datetime"] = pd.to_datetime(df_api["tpep_pickup_datetime"])

ic(df_api.shape)

# %%
df_api.head(3)

# %% [markdown]
# ### Пагінація через `$offset`
#
# Socrata повертає максимум 1000 рядків за запит.
# Для більшого обсягу — посторінкова вибірка.

# %%
PAGE_SIZE = 500
MAX_PAGES = 3
page_params = {
    "$select": "PULocationID,DOLocationID,trip_distance,fare_amount,tpep_pickup_datetime",
    "$where": "trip_distance > 5 AND fare_amount > 0",
    "$order": "tpep_pickup_datetime ASC",
}

pages = []
for page in range(MAX_PAGES):
    r = requests.get(
        f"{SOCRATA_BASE}/{DATASET_ID}.json",
        params={**page_params, "$limit": PAGE_SIZE, "$offset": page * PAGE_SIZE},
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    batch = r.json()
    if not batch:
        break
    pages.append(pd.DataFrame(batch))
    ic(page, len(batch))

df_paged = pd.concat(pages, ignore_index=True)
ic(len(df_paged))

# %% [markdown]
# ### Довідник зон — збереження у Parquet
#
# Зберігаємо Parquet замість CSV:
# - менший розмір завдяки колонковому стисненню
# - типи зберігаються явно (не потрібен зайвий cast при читанні)
# - predicate pushdown у downstream запитах

# %%
ZONE_PARQUET = SOURCE_DIR / "taxi_zone_lookup.parquet"
ZONE_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

r_zones = requests.get(ZONE_CSV_URL, timeout=30)
r_zones.raise_for_status()
df_zones = pd.read_csv(StringIO(r_zones.text))
df_zones.to_parquet(ZONE_PARQUET, index=False)

csv_kb = len(r_zones.content) / 1e3
parquet_kb = ZONE_PARQUET.stat().st_size / 1e3
ic(csv_kb, parquet_kb)

# %% [markdown]
# ### Join: API-дані + довідник зон
#
# `PULocationID` з Socrata збігається з `LocationID` у zone lookup —
# той самий ключ, що і в основному Parquet-датасеті (розділ 2).

# %%
zones = pd.read_parquet(ZONE_PARQUET).rename(
    columns={"LocationID": "PULocationID", "Zone": "PU_Zone"}
)[["PULocationID", "PU_Zone"]]

df_api_joined = df_api.merge(zones, on="PULocationID", how="left")
df_api_joined[["PU_Zone", "fare_amount", "trip_distance"]].head(5)

# %% [markdown]
# Топ зон відправлення у вибірці:

# %%
df_api_joined["PU_Zone"].value_counts().head(10)



# %% [markdown]
# ## 2. Завантаження основного датасету
#
# NYC TLC Yellow Taxi Trip Records за January 2024.
# Файл: `data/landing/yellow_tripdata_2024-01.parquet` (~100 MB, ~3 млн рядків)
#
# Скрипт **ідемпотентний** — повторний запуск нічого не перезаписує.

# %%
YEAR, MONTH = 2024, 1
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
PARQUET_PATH = SOURCE_DIR / f"yellow_tripdata_{YEAR}-{MONTH:02d}.parquet"
url = f"{BASE_URL}/{PARQUET_PATH.name}"

if PARQUET_PATH.exists():
    ic(PARQUET_PATH, PARQUET_PATH.stat().st_size / 1e6)
else:
    ic(url)
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(PARQUET_PATH, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    ic(PARQUET_PATH, PARQUET_PATH.stat().st_size / 1e6)



# %% [markdown]
# Дані можна було б не зберігати на локальному диску - іноді такий підхід теж корисний. `pandas` надає можливість читати дані прямо з URL:

# %%
pd.read_parquet(url).info()

# %% [markdown]
# ## 3. Інспекція схеми без завантаження даних
#
# `pq.read_schema` читає тільки **footer** Parquet-файлу — метадані колонок,
# але не самі рядки. Дешева перевірка перед початком роботи.

# %%
import pyarrow.parquet as pq

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

# %%
ic(len(actual_cols), missing or "немає")

# %% [markdown]
# Повна схема:

# %%
actual_schema



# %% [markdown]
# ## 4. Pandas: читання Parquet
#
# `pd.read_parquet` зчитує весь файл у пам'ять (**eager evaluation**).

# %%
df = pd.read_parquet(PARQUET_PATH)

# %%
ic(df.shape, df.memory_usage(deep=True).sum() / 1e6)

# %%
df.dtypes

# %% [markdown]
# За замовчуванням, `pandas` використовує `numpy` для репрезентації даних в **оперативній памʼяті**.

# %%
type(df.dtypes["VendorID"])

# %% [markdown]
# ### Arrow-backed pandas (pandas 2.x)
#
# `dtype_backend="pyarrow"` зберігає колонки в форматі Apache Arrow **всередині оперативної памʼяті**. 
#
# Поширений міф: «Arrow завжди зменшує пам'ять у pandas». Насправді — **не завжди**.
# `memory_usage(deep=True)` може показати більше для Arrow-backend через Arrow-metadata overhead.
# Виграш залежить від типів і розподілу даних; для числових датасетів різниця мінімальна.
#
# Реальні переваги Arrow-backend у pandas:
#
# 1. **Багатший тип-системи**: nullable integers без float-кастингу (`int64[pyarrow]`, не `float64`),
#    точний `decimal128` для фінансових розрахунків, нативні `date32` / `time32`.
# 2. **Інтероперабельність**: Polars нативно читає Arrow-буфери — конвертація zero-copy,
#    без зайнього копіювання даних у пам'яті.

# %%
df_arrow = pd.read_parquet(PARQUET_PATH, dtype_backend="pyarrow")
mem_default = df.memory_usage(deep=True).sum() / 1e6
mem_arrow = df_arrow.memory_usage(deep=True).sum() / 1e6

# %%
ic(mem_default, mem_arrow)

# %% [markdown]
# Arrow dtypes (порівняйте `passenger_count`: `float64` → `int64[pyarrow]`):

# %%
df_arrow.dtypes.head(6)

# %%
type(df_arrow.dtypes["VendorID"])

# %% [markdown]
# ## 5. Polars: lazy evaluation

# %%
import sys
import time
import polars as pl

# %% [markdown]
# ### Arrow → Polars: zero-copy конвертація
#
# Polars нативно читає Arrow-буфери. З numpy-backend pandas змушений копіювати дані;
# з Arrow-backend — ні. Це практичний сценарій, де Arrow-backend у pandas виправданий.

# %%
t0 = time.perf_counter()
_ = pl.from_pandas(df)
t_numpy_conv = time.perf_counter() - t0

t0 = time.perf_counter()
_ = pl.from_pandas(df_arrow)
t_arrow_conv = time.perf_counter() - t0

# %%
ic(t_numpy_conv, t_arrow_conv)
ic(t_numpy_conv / t_arrow_conv)

# %% [markdown]
# ### Lazy evaluation
#
# `pl.scan_parquet` **не читає дані** — будує LazyFrame (план запиту).
# Дані матеріалізуються тільки при `.collect()`.
#
# Одна з головних переваг це можливість проаналізувати трансформації що відбуваються в скрипті та оптимізувати їх заздалегідь.

# %%
lazy = pl.scan_parquet(PARQUET_PATH)

# %%
ic(sys.getsizeof(lazy))

# %% [markdown]
# Також така парадигма роботи дозволяє Polars використовувати нативні для Parquet або інших джерел даних оптимізації як **predicate pushdown** (фільтрація даних ДО зчитування).

# %%
filtered_lazy = (
    lazy
    .filter(pl.col("fare_amount") > 0)
    .filter(pl.col("passenger_count") > 0)
    .select(["tpep_pickup_datetime", "PULocationID", "fare_amount", "trip_distance"])
)

# %% [markdown]
# Оптимізований план запиту (видно predicate pushdown):

# %%
print(filtered_lazy.explain(optimized=True))

# %%
filtered_lazy.show_graph(optimized=True)

# %% [markdown]
# Для того щоб виконати обчислення (прочитати дані з фільтром у даному випадку) треба викликати функцію `collect`.

# %%
df_pl = filtered_lazy.collect()

# %%
ic(df_pl.shape, sys.getsizeof(df_pl) / 1e6)

# %%
df_pl.head(3)



# %% [markdown]
# ## 6. Benchmark: Pandas vs Polars
#
# Та сама задача для обох бібліотек:
# - `filter(fare_amount > 0)`
# - `groupby(PULocationID).mean(fare_amount)`
#
# **Predicate pushdown** тут показує себе в усій красі

# %%
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

# %%
pd.DataFrame({
    "Бібліотека": ["Pandas", "Polars"],
    "Час (с)": [round(t_pandas, 2), round(t_polars, 2)],
    "Зон": [len(result_pd), len(result_pl)],
})

# %% [markdown]
# **Важливе зауваження**: `polars` не є гарантією того що Ви завжди будете мати blazingly fast data processing (наприклад, нижче ми будемо постійно пере-скановувати dataframe, через що швидкість може падати). Але за умови правильного використання ви зможете як ефективно використовувати памʼять, так й ефективніше використовувати CPU.

# %% [markdown]
# ## 7. Основні операції: Pandas vs Polars
#
# Кожну операцію показуємо в обох бібліотеках.
# Pandas — eager, знайомий синтаксис.
# Polars — lazy за замовчуванням, виразний API, швидший на великих даних.

# %%
lf = pl.scan_parquet(PARQUET_PATH)

# %% [markdown]
# ### Filter
#
# Залишаємо тільки денні поїздки (7:00–18:59).

# %%
daytime_pd = df[
    (df["tpep_pickup_datetime"].dt.hour >= 7) &
    (df["tpep_pickup_datetime"].dt.hour < 19)
]
ic(len(daytime_pd))

# %%
daytime_pl = (
    lf.filter(pl.col("tpep_pickup_datetime").dt.hour().is_between(7, 18))
    .collect()
)
ic(len(daytime_pl))

# %% [markdown]
# ### Add column
#
# Додаємо `hour` і `is_weekend` на основі часу відправлення.

# %%
df["hour"] = df["tpep_pickup_datetime"].dt.hour
df["is_weekend"] = df["tpep_pickup_datetime"].dt.dayofweek >= 5
df[["tpep_pickup_datetime", "hour", "is_weekend"]].head(3)

# %%
df_with_cols = (
    lf.with_columns([
        pl.col("tpep_pickup_datetime").dt.hour().alias("hour"),
        (pl.col("tpep_pickup_datetime").dt.weekday() >= 5).alias("is_weekend"),
    ])
    .collect()
)
df_with_cols[["tpep_pickup_datetime", "hour", "is_weekend"]].head(3)

# %% [markdown]
# ### Join
#
# Додаємо назву зони відправлення (`PU_Zone`) з довідника.
# Довідник уже в Parquet — зберегли його на кроці 1 (REST API завантаження).

# %%
zones_pd = pd.read_parquet(ZONE_PARQUET).rename(
    columns={"LocationID": "PULocationID", "Zone": "PU_Zone"}
)[["PULocationID", "PU_Zone"]]
df_joined_pd = df.merge(zones_pd, on="PULocationID", how="left")
df_joined_pd["PU_Zone"].value_counts().head(5)

# %%
zones_pl = pl.read_parquet(ZONE_PARQUET).rename(
    {"LocationID": "PULocationID", "Zone": "PU_Zone"}
).select(["PULocationID", "PU_Zone"])
df_joined_pl = lf.join(zones_pl.lazy(), on="PULocationID", how="left").collect()
df_joined_pl["PU_Zone"].value_counts().head(5)

# %% [markdown]
# ### Group by
#
# Середній тариф і кількість поїздок по годині доби.

# %%
by_hour_pd = (
    df.groupby("hour")["fare_amount"]
    .agg(["mean", "count"])
    .reset_index()
    .sort_values("hour")
)
by_hour_pd

# %%
by_hour_lf = (lf.with_columns(pl.col("tpep_pickup_datetime").dt.hour().alias("hour"))
    .group_by("hour")
    .agg([
        pl.col("fare_amount").mean().alias("mean"),
        pl.col("fare_amount").count().alias("count"),
    ])
    .sort("hour"))

# %%
by_hour_lf.show_graph(optimized=True)

# %%
by_hour_pl = (
    by_hour_lf 
    .collect()
)
by_hour_pl

# %% [markdown]
# ### Window function
#
# Rolling 3-годинне середнє по погодинній виручці.

# %%
hourly_pd = df.groupby("hour")["fare_amount"].sum().reset_index().sort_values("hour")
hourly_pd["rolling_3h_avg"] = hourly_pd["fare_amount"].rolling(window=3, min_periods=1).mean()
hourly_pd

# %%
hourly_lf = (lf.with_columns(pl.col("tpep_pickup_datetime").dt.hour().alias("hour"))
    .group_by("hour")
    .agg(pl.col("fare_amount").sum().alias("total_fare"))
    .sort("hour")
    .with_columns(
        pl.col("total_fare").rolling_mean(window_size=3, min_samples=1).alias("rolling_3h_avg")
    ))

# %%
hourly_lf.show_graph(optimized=True)

# %%
hourly_pl = (
    hourly_lf
    .collect()
)
hourly_pl

# %% [markdown]
# ### Union (concat)
#
# Симулюємо об'єднання двох місяців — зсуваємо Jan на ~1 місяць для демо.

# %%
jan_pd = pd.read_parquet(PARQUET_PATH)
feb_pd = jan_pd.sample(100_000, random_state=42).copy()
feb_pd["tpep_pickup_datetime"] = feb_pd["tpep_pickup_datetime"] + pd.DateOffset(months=1)
feb_pd["tpep_dropoff_datetime"] = feb_pd["tpep_dropoff_datetime"] + pd.DateOffset(months=1)
combined_pd = pd.concat([jan_pd, feb_pd], ignore_index=True)
ic(len(jan_pd), len(feb_pd), len(combined_pd))

# %%
jan_pl = pl.read_parquet(PARQUET_PATH)
feb_pl = jan_pl.sample(100_000, seed=42).with_columns([
    pl.col("tpep_pickup_datetime").dt.offset_by("1mo"),
    pl.col("tpep_dropoff_datetime").dt.offset_by("1mo"),
])
combined_pl = pl.concat([jan_pl, feb_pl])
ic(len(jan_pl), len(feb_pl), len(combined_pl))



# %% [markdown]
# ## 8. EDA: пошук первинного ключа
#
# Чи є природний унікальний ідентифікатор поїздки?

# %%
lf.head(10).collect()

# %%
candidate = ["VendorID","tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID", "fare_amount"]

(
    lf.select(candidate)
      .group_by(candidate)
      .len()
      .filter(pl.col("len") > 1)
      .collect()
)

# %%
duplicated_rows = lf.filter(
    pl.struct(lf.columns[:-3]).is_duplicated()
)

# %%
lf.columns[:-3]

# %%
duplicated_rows.collect()

# %% [markdown]
# ### Аналіз типів даних
#
# Перевіряємо типи колонок — чи відповідають вони семантиці поля?

# %%
df.dtypes

# %% [markdown]
# `PULocationID` — int, `fare_amount` — float64 (USD), `passenger_count` — float64 через nullable (краще було б Int8).

# %% [markdown]
# ### DQ по п'яти осях
#
# #### Accuracy — чи значення відповідають реальності?

# %%
neg_fares = df[df["fare_amount"] < 0]
ic(len(neg_fares))

# %%
df["duration_h"] = (
    df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
).dt.total_seconds() / 3600
df["speed_mph"] = df["trip_distance"] / df["duration_h"].replace(0, float("nan"))
fast = df[df["speed_mph"] > 200]
ic(len(fast))

# %% [markdown]
# #### Completeness — відсутні значення

# %%
nulls = df.isnull().sum()
nulls[nulls > 0]

# %% [markdown]
# #### Consistency — внутрішня узгодженість

# %%
time_anomaly = df[df["tpep_pickup_datetime"] >= df["tpep_dropoff_datetime"]]
ic(len(time_anomaly))

# %% [markdown]
# #### Timeliness — чи дати в очікуваному діапазоні?

# %%
ic(df["tpep_pickup_datetime"].min(), df["tpep_pickup_datetime"].max())

# %%
out_of_range = df[df["tpep_pickup_datetime"].dt.year != 2024]
ic(len(out_of_range))

# %% [markdown]
# #### Validity — значення в допустимих межах?

# %%
zero_pax = df[df["passenger_count"].isin([0]) | df["passenger_count"].isna()]
ic(len(zero_pax))

# %%
invalid_pu = df[(df["PULocationID"] > 265) | (df["PULocationID"] < 1)]
ic(len(invalid_pu))

# %% [markdown]
# ### Статистичний аналіз

# %%
df[["fare_amount", "trip_distance", "passenger_count", "duration_h"]].describe()

# %% [markdown]
# ### Візуалізація (seaborn)
#
# Фільтруємо очевидні аномалії для кращої читаності графіків.

# %%
import matplotlib.pyplot as plt
import seaborn as sns

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



# %% [markdown]
# ## 9. Геопросторовий вимір (GeoPandas)
#
# NYC TLC надає межи таксі-зон у форматі Shapefile (ZIP).
# GeoPandas дозволяє робити **spatial joins** — та сама логіка, що й у звичайних DataFrame.
#
# Завантажуємо Shapefile з TLC CloudFront,
# об'єднуємо з кількістю поїздок і будуємо хороплет-карту.

# %%
import io
import os
import tempfile
import zipfile
import geopandas as gpd

ZONES_ZIP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

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

# %%
ic(gdf_zones.shape)

# %%
gdf_zones[["location_id", "zone", "borough", "geometry"]].head(3)

# %%
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
