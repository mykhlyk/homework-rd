"""Gold stage — three analytics tables built from silver.

TODO (Завдання 4, 5, 6): реалізуйте три функції нижче.
Контракт: див. CONTRACTS.md → "gold repo_activity", "gold activity_per_minute",
"gold push_commits_by_repo". Усі лічильники приводьте до Int64 (.cast(pl.Int64)),
щоб схема результату була стабільною.

  * build_repo_activity:        кількість подій + кількість унікальних типів на repo
  * build_activity_per_minute:  кількість подій по хвилинах (.dt.truncate("1m"))
  * build_push_commits_by_repo: тільки PushEvent — кількість пушів і сума commit_count на repo
"""

from __future__ import annotations
import os

import polars as pl

from . import config


def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:
    os.makedirs(os.path.dirname(config.GOLD_REPO_ACTIVITY), exist_ok=True)
 
    repo_activity = (
        silver
        .group_by("repo_name")
        .agg(
            pl.len().cast(pl.Int64).alias("event_count"),
            pl.col("event_type").n_unique().cast(pl.Int64).alias("distinct_event_types"),
        )
        .sort("event_count", descending=True)
    )
    #print(repo_activity)
    repo_activity.write_parquet(config.GOLD_REPO_ACTIVITY)
 
    return repo_activity

def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    os.makedirs(os.path.dirname(config.GOLD_ACTIVITY_PER_MINUTE), exist_ok=True)
 
    activity_per_minute = (
        silver
        .with_columns(pl.col("created_at").dt.truncate("1m").alias("minute"))
        .group_by("minute")
        .agg(pl.len().cast(pl.Int64).alias("event_count"))
        .sort("minute")
    )
    #print(activity_per_minute)
    activity_per_minute.write_parquet(config.GOLD_ACTIVITY_PER_MINUTE)
 
    return activity_per_minute

def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    os.makedirs(os.path.dirname(config.GOLD_PUSH_COMMITS), exist_ok=True)
 
    push_commits = (
        silver
        .filter(pl.col("event_type") == "PushEvent")
        .group_by("repo_name")
        .agg(
            pl.len().cast(pl.Int64).alias("push_events"),
            pl.col("commit_count").sum().cast(pl.Int64).alias("total_commits"),
        )
    )
    #print(push_commits)
    push_commits.write_parquet(config.GOLD_PUSH_COMMITS)
 
    return push_commits