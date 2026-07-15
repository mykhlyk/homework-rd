"""Shared configuration for the GitHub Archive ETL pipeline.

Single source of truth for the data source, the landing schema (the read
contract) and the on-disk layout. Importing modules never hard-code paths or
event-type lists — they read them from here.
"""

from __future__ import annotations

import polars as pl

# --- Data source -----------------------------------------------------------
# One immutable hour of GitHub public events (2024-01-15, 14:00–14:59 UTC).
# The file is historical and never changes, so every run sees identical data.
GH_HOUR = "2024-01-15-14"
GH_URL = f"https://data.gharchive.org/{GH_HOUR}.json.gz"

# Event types we keep in the silver layer.
TARGET_EVENT_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "IssueCommentEvent",
]

# --- Read contract ---------------------------------------------------------
# The raw record carries a huge, heterogeneous `payload` object. We declare an
# explicit narrow schema so Polars reads ONLY the fields we need and ignores the
# rest — this is the data contract for the source, and it keeps the read fast
# and stable across the whole file.
LANDING_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.String,
    "type": pl.String,
    "actor": pl.Struct({"id": pl.Int64, "login": pl.String}),
    "repo": pl.Struct({"id": pl.Int64, "name": pl.String}),
    "created_at": pl.String,
    "public": pl.Boolean,
    "payload": pl.Struct(
        {
            "action": pl.String,
            "commits": pl.List(pl.Struct({"sha": pl.String})),
        }
    ),
}

# --- On-disk layout --------------------------------------------------------
# Усі шляхи відносні до кореня homework/ — запускайте pipeline звідти
# (uv run python run.py).
DATA_DIR = "data"

LANDING_DIR = f"{DATA_DIR}/landing"
LANDING_FILE = f"{LANDING_DIR}/{GH_HOUR}.json.gz"

BRONZE_FILE = f"{DATA_DIR}/bronze/events.parquet"
SILVER_FILE = f"{DATA_DIR}/silver/events.parquet"
SILVER_PARTITIONED_DIR = f"{DATA_DIR}/silver/events_by_type"

GOLD_REPO_ACTIVITY = f"{DATA_DIR}/gold/repo_activity.parquet"
GOLD_ACTIVITY_PER_MINUTE = f"{DATA_DIR}/gold/activity_per_minute.parquet"
GOLD_PUSH_COMMITS = f"{DATA_DIR}/gold/push_commits_by_repo.parquet"
