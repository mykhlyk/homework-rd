"""Bronze: landing-зона -> bronze.raw_events у Postgres. ЕТАП 1.

Spark у local mode читає NDJSON із landing і дописує рядки у сховище через JDBC.

Контракт шару Bronze (повністю — SPEC.md, розділ 3):
  * читаємо з ЯВНОЮ схемою (ніякого inferSchema: схема — це контракт, а не здогадка);
  * `payload` лишається СИРИМ JSON-рядком: Bronze нічого не парсить і нічого не виправляє;
  * ідемпотентність за файлом: файл, який уже завантажено, вдруге не потрапляє;
  * атомарний append: або всі нові рядки в таблиці, або жодного.

    uv run python bronze_job.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common import config
from common.spark import build_spark

# явна схема конверта події: event_id, event_type, ride_id, occurred_at, source, payload.
# Підказка: `payload` — вкладений обʼєкт; якщо оголосити його як StringType, Spark віддасть
# його текст як є, без розбору.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("ride_id", StringType()),
        StructField("occurred_at", TimestampType()),
        StructField("source", StringType()),
        StructField("payload", StringType()),
    ]
)


def read_landing(spark: SparkSession, landing_dir: str) -> DataFrame:
    """Сирі події з landing плюс метадані ingestion.

    прочитайте `*.ndjson` з `landing_dir` (рекурсивно; `*.ndjson.tmp` — це in-flight
    файли consumer-а, їх читати не можна) з `EVENT_SCHEMA`, і поверніть колонки:
      event_id, event_type, ride_id, occurred_at (timestamp), source, payload (СИРИЙ текст),
      _source_file (шлях ВІДНОСНО landing/, напр. `dt=…/hour=…/part-….ndjson`),
      _ingested_at (current_timestamp).
    `dt=` і `hour=` у шляху — час запису файлу, а не бізнес-колонки: колонками стати не мають.
    """
    raw = (
        spark.read.schema(EVENT_SCHEMA)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.ndjson")
        .json(landing_dir)
    )
    return raw.select(
        "event_id",
        "event_type",
        "ride_id",
        "occurred_at",
        "source",
        "payload",
        F.regexp_replace(F.input_file_name(), "^.*/landing/", "").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def loaded_files(spark: SparkSession) -> set[str]:
    """Файли, які вже лежать у Bronze (ключ ідемпотентності). ДАНО."""
    query = f"(SELECT DISTINCT _source_file FROM {config.BRONZE_TABLE}) AS t"
    rows = spark.read.jdbc(config.JDBC_URL, query, properties=config.JDBC_PROPERTIES).collect()
    return {r["_source_file"] for r in rows}


def select_new(df: DataFrame, already_loaded: set[str]) -> DataFrame:
    """Лишає рядки з файлів, яких ще нема в Bronze.

    відфільтруйте за `_source_file`. Порожній `already_loaded` — це перший запуск.
    """
    if not already_loaded:
        return df
    return df.filter(~F.col("_source_file").isin(list(already_loaded)))


def write_bronze(df: DataFrame) -> None:
    """Append у `config.BRONZE_TABLE` ОДНІЄЮ транзакцією.

    запишіть через JDBC (`config.JDBC_URL`, `config.JDBC_PROPERTIES`) у режимі append.
    Вимога — «або все, або нічого»: збій посеред запису не лишає в таблиці частини батча.
    Подумайте, скільки транзакцій відкриває Spark при JDBC-записі й від чого це залежить.
    """
    (
        df.coalesce(1).write.jdbc(
            url=config.JDBC_URL,
            table=config.BRONZE_TABLE,
            mode="append",
            properties=config.JDBC_PROPERTIES,
        )
    )


def main() -> None:
    """ДАНО."""
    spark = build_spark("bronze-job")
    try:
        raw = read_landing(spark, str(config.LANDING_DIR))
        new = select_new(raw, loaded_files(spark)).cache()

        n_new = new.count()
        if n_new == 0:
            print("Bronze: нових файлів немає — пропускаю запис (ідемпотентно).")
        else:
            n_files = new.select("_source_file").distinct().count()
            write_bronze(new)
            print(f"Bronze: додано {n_new} подій із {n_files} файлів.")

        total = spark.read.jdbc(
            config.JDBC_URL,
            f"(SELECT count(*) AS n FROM {config.BRONZE_TABLE}) AS t",
            properties=config.JDBC_PROPERTIES,
        ).collect()[0]["n"]
        print(f"Bronze total: {total}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
