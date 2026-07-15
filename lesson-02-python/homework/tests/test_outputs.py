"""Acceptance tests — one block per task.

Every assertion looks at the produced Parquet files/directories, never at your
functions. If a test fails, read its name: it tells you which task and which
part of the contract is off. The magic numbers are deterministic because the
source hour (2024-01-15-14) is immutable.
"""

from __future__ import annotations

import polars as pl

TARGET_EVENT_TYPES = {
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "IssueCommentEvent",
}

BRONZE_SCHEMA = {
    "event_id": pl.String,
    "event_type": pl.String,
    "actor_id": pl.Int64,
    "actor_login": pl.String,
    "repo_id": pl.Int64,
    "repo_name": pl.String,
    "created_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "public": pl.Boolean,
    "action": pl.String,
    "commit_count": pl.Int64,
}


# --- Task 1: bronze --------------------------------------------------------

def test_bronze_schema(bronze: pl.DataFrame):
    assert dict(bronze.schema) == BRONZE_SCHEMA


def test_bronze_row_count(bronze: pl.DataFrame):
    assert bronze.height == 267_250


def test_bronze_keeps_all_event_types(bronze: pl.DataFrame):
    # Bronze does NOT filter: more than the five target types are present.
    assert bronze["event_type"].n_unique() > len(TARGET_EVENT_TYPES)


def test_bronze_created_at_is_utc(bronze: pl.DataFrame):
    assert bronze.schema["created_at"] == pl.Datetime("us", "UTC")
    assert bronze["created_at"].null_count() == 0


# --- Task 2: silver --------------------------------------------------------

def test_silver_schema(silver: pl.DataFrame):
    assert dict(silver.schema) == BRONZE_SCHEMA


def test_silver_row_count(silver: pl.DataFrame):
    assert silver.height == 211_466


def test_silver_only_target_types(silver: pl.DataFrame):
    assert set(silver["event_type"].unique()) == TARGET_EVENT_TYPES


def test_silver_no_null_keys(silver: pl.DataFrame):
    for col in ("event_id", "created_at", "repo_name"):
        assert silver[col].null_count() == 0, col
    assert (silver["repo_name"] == "").sum() == 0


def test_silver_unique_event_id(silver: pl.DataFrame):
    assert silver["event_id"].n_unique() == silver.height


# --- Task 3: silver partitioned by event_type ------------------------------

def test_partition_directories(silver_partitioned: pl.DataFrame):
    # event_type is recovered from the Hive directory names on read.
    assert set(silver_partitioned["event_type"].unique()) == TARGET_EVENT_TYPES


def test_partition_roundtrip_row_count(silver_partitioned: pl.DataFrame):
    assert silver_partitioned.height == 211_466


# --- Task 4: gold repo_activity --------------------------------------------

def test_repo_activity_schema(repo_activity: pl.DataFrame):
    assert dict(repo_activity.schema) == {
        "repo_name": pl.String,
        "event_count": pl.Int64,
        "distinct_event_types": pl.Int64,
    }


def test_repo_activity_totals(repo_activity: pl.DataFrame):
    assert repo_activity.height == 67_143
    # No event is lost or double-counted relative to silver.
    assert repo_activity["event_count"].sum() == 211_466


def test_repo_activity_sorted_desc(repo_activity: pl.DataFrame):
    counts = repo_activity["event_count"].to_list()
    assert counts == sorted(counts, reverse=True)


# --- Task 5: gold activity_per_minute --------------------------------------

def test_activity_per_minute_schema(activity_per_minute: pl.DataFrame):
    assert activity_per_minute.schema["minute"] == pl.Datetime("us", "UTC")
    assert activity_per_minute.schema["event_count"] == pl.Int64


def test_activity_per_minute_is_full_hour(activity_per_minute: pl.DataFrame):
    assert activity_per_minute.height == 60
    assert activity_per_minute["event_count"].sum() == 211_466


# --- Task 6: gold push_commits_by_repo -------------------------------------

def test_push_commits_schema(push_commits: pl.DataFrame):
    assert dict(push_commits.schema) == {
        "repo_name": pl.String,
        "push_events": pl.Int64,
        "total_commits": pl.Int64,
    }


def test_push_commits_totals(push_commits: pl.DataFrame):
    assert push_commits.height == 52_759
    assert push_commits["total_commits"].sum() == 232_047
