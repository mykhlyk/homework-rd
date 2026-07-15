"""Shared fixtures for the homework tests.

The tests check the DATA your pipeline produces under homework/data/, not your
Python code. Run your pipeline first, then run the tests:

    uv run python run.py
    uv run pytest
"""

from __future__ import annotations

import os

import polars as pl
import pytest

# Шляхи відносні до кореня homework/ — запускайте pytest звідти.
DATA_DIR = "data"

BRONZE_FILE = f"{DATA_DIR}/bronze/events.parquet"
SILVER_FILE = f"{DATA_DIR}/silver/events.parquet"
SILVER_PARTITIONED_DIR = f"{DATA_DIR}/silver/events_by_type"
GOLD_REPO_ACTIVITY = f"{DATA_DIR}/gold/repo_activity.parquet"
GOLD_ACTIVITY_PER_MINUTE = f"{DATA_DIR}/gold/activity_per_minute.parquet"
GOLD_PUSH_COMMITS = f"{DATA_DIR}/gold/push_commits_by_repo.parquet"


def _require(path: str) -> str:
    if not os.path.exists(path):
        pytest.fail(
            f"Очікуваний артефакт відсутній: {path}\n"
            f"Спочатку запустіть pipeline (uv run python run.py), потім тести."
        )
    return path


@pytest.fixture(scope="session")
def bronze() -> pl.DataFrame:
    return pl.read_parquet(_require(BRONZE_FILE))


@pytest.fixture(scope="session")
def silver() -> pl.DataFrame:
    return pl.read_parquet(_require(SILVER_FILE))


@pytest.fixture(scope="session")
def silver_partitioned() -> pl.DataFrame:
    _require(SILVER_PARTITIONED_DIR)
    return pl.read_parquet(SILVER_PARTITIONED_DIR, hive_partitioning=True)


@pytest.fixture(scope="session")
def repo_activity() -> pl.DataFrame:
    return pl.read_parquet(_require(GOLD_REPO_ACTIVITY))


@pytest.fixture(scope="session")
def activity_per_minute() -> pl.DataFrame:
    return pl.read_parquet(_require(GOLD_ACTIVITY_PER_MINUTE))


@pytest.fixture(scope="session")
def push_commits() -> pl.DataFrame:
    return pl.read_parquet(_require(GOLD_PUSH_COMMITS))
