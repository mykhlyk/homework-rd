"""Bronze stage — read the raw NDJSON and flatten it to one wide table.

TODO (Завдання 1): реалізуйте build_bronze().
Контракт колонок та типів: див. CONTRACTS.md → "bronze".

Підказки:
  * читайте NDJSON ліниво: pl.scan_ndjson(config.LANDING_FILE, schema=config.LANDING_SCHEMA)
  * розгортайте вкладені структури через .struct.field("...")
  * created_at -> datetime: .str.to_datetime("%Y-%m-%dT%H:%M:%SZ", time_zone="UTC")
  * commit_count: довжина списку payload.commits; для не-PushEvent коміти
    відсутні -> заповніть 0 (.list.len().fill_null(0))
  * запишіть результат у config.BRONZE_FILE (Parquet) і поверніть DataFrame
"""

from __future__ import annotations

import polars as pl

from . import config

import os


def build_bronze() -> pl.DataFrame:
    os.makedirs(os.path.dirname(config.BRONZE_FILE), exist_ok=True)

    lf = (
        pl.scan_ndjson(
            config.LANDING_FILE,
            schema=config.LANDING_SCHEMA,
        )
        .select(
            pl.col("id").alias("event_id"),
            pl.col("type").alias("event_type"),
            pl.col("actor").struct.field("id").alias("actor_id"),
            pl.col("actor").struct.field("login").alias("actor_login"),
            pl.col("repo").struct.field("id").alias("repo_id"),
            pl.col("repo").struct.field("name").alias("repo_name"),
            pl.col("created_at").str.to_datetime(
                "%Y-%m-%dT%H:%M:%SZ",
                time_zone="UTC",
            ),
            pl.col("public"),
            pl.col("payload").struct.field("action").alias("action"),
            (
                pl.col("payload")
                .struct.field("commits")
                .list.len()
                .fill_null(0)
                .cast(pl.Int64)
                .alias("commit_count")
            ),
        )
    )

    #print (lf)
    lf.sink_parquet(config.BRONZE_FILE)

    return pl.read_parquet(config.BRONZE_FILE)