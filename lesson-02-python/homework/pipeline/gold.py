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

import polars as pl

from . import config


def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:
    raise NotImplementedError("Завдання 4: реалізуйте repo_activity згідно з CONTRACTS.md")


def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    raise NotImplementedError("Завдання 5: реалізуйте activity_per_minute згідно з CONTRACTS.md")


def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    raise NotImplementedError("Завдання 6: реалізуйте push_commits_by_repo згідно з CONTRACTS.md")
