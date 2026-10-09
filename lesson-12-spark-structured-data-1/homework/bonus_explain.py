
from __future__ import annotations

import contextlib
import io
import time
from pathlib import Path

import job
from pyspark.sql import DataFrame


def plan_text(df: DataFrame) -> str:
    """Фізичний план як рядок (explain('formatted') друкує у stdout)."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain("formatted")
    return buf.getvalue()


def bench(label: str, df: DataFrame, plan_file: str) -> None:
    plan = plan_text(df)
    exchanges = plan.count("Exchange")
    Path(plan_file).write_text(plan, encoding="utf-8")

    start = time.perf_counter()
    rows = df.count()
    elapsed = time.perf_counter() - start

    print(f"\n===== {label} =====")
    print(f"рядків:        {rows}")
    print(f"Exchange у плані: {exchanges}")
    print(f"час .count():   {elapsed:.3f} s")
    print(f"план збережено:  {plan_file}")


def main() -> None:
    spark = job.build_spark("l12-bonus")
    spark.sparkContext.setLogLevel("ERROR")

    events = job.with_derived(job.clean(job.flatten(job.read_raw(spark)))).cache()
    events.count()  # прогрів кешу — щоб час не включав читання landing

    union_df = job.build_summary(events, job.SUMMARY_DIMENSIONS)
    onepass_df = job.build_summary_one_pass(events, job.SUMMARY_DIMENSIONS)

    # обидва мають дати однаковий підсумок
    print("однакова к-сть рядків:",
          union_df.count() == onepass_df.count() == 25102)

    bench("unionByName (4 сканування)", union_df, "plan_union.txt")
    bench("explode (1 прохід)", onepass_df, "plan_onepass.txt")

    events.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()