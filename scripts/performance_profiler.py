#!/usr/bin/env python3
"""Scalability profiler for strong/weak scaling experiments."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

from scripts.run_pipeline import create_spark_session, ensure_dirs


def timed_aggregation(df) -> float:
    start = time.perf_counter()
    _ = df.groupBy("crash_year").agg(F.count("*").alias("rows"), F.sum(F.coalesce(F.col("injuries_total").cast("long"), F.lit(0))).alias("inj")).collect()
    return time.perf_counter() - start


def strong_scaling(clean_df, partitions: list[int], fixed_rows: int) -> pd.DataFrame:
    rows = []
    base = clean_df.limit(fixed_rows)
    for p in partitions:
        t = timed_aggregation(base.repartition(p).cache())
        rows.append({"mode": "strong", "rows": fixed_rows, "partitions": p, "seconds": t})
    return pd.DataFrame(rows)


def weak_scaling(clean_df, partitions: list[int], rows_per_partition: int) -> pd.DataFrame:
    rows = []
    for p in partitions:
        n_rows = p * rows_per_partition
        subset = clean_df.limit(n_rows).repartition(p).cache()
        t = timed_aggregation(subset)
        rows.append({"mode": "weak", "rows": n_rows, "partitions": p, "seconds": t})
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Spark strong/weak scaling experiments")
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--config", type=Path, default=Path("config/spark_config.yaml"))
    parser.add_argument("--partitions", type=str, default="2,4,8")
    parser.add_argument("--fixed-rows", type=int, default=300000)
    parser.add_argument("--rows-per-partition", type=int, default=100000)
    parser.add_argument("--cost-per-core-hour", type=float, default=0.06)
    args = parser.parse_args()

    base_dir = args.output_dir.resolve()
    dirs = ensure_dirs(base_dir)
    reports_dir = dirs["reports"]

    spark = create_spark_session(base_dir, args.config)
    try:
        clean_df = spark.read.parquet(str(dirs["processed"] / "crashes_clean"))
        partitions = [int(x.strip()) for x in args.partitions.split(",") if x.strip()]

        strong_df = strong_scaling(clean_df, partitions, args.fixed_rows)
        weak_df = weak_scaling(clean_df, partitions, args.rows_per_partition)

        strong_df.to_csv(reports_dir / "scaling_strong.csv", index=False)
        weak_df.to_csv(reports_dir / "scaling_weak.csv", index=False)

        merged = pd.concat([strong_df, weak_df], ignore_index=True)
        merged["core_hours"] = (merged["seconds"] / 3600.0) * merged["partitions"]
        merged["estimated_cost"] = merged["core_hours"] * args.cost_per_core_hour
        merged["rows_per_second"] = merged["rows"] / merged["seconds"]
        merged.to_csv(reports_dir / "scaling_cost_performance.csv", index=False)

        bottlenecks = {
            "likely_io_bound_when": "rows_per_second does not improve with more partitions",
            "likely_shuffle_bound_when": "strong scaling speedup flattens after partition increases",
            "likely_compute_bound_when": "CPU saturation observed with high executor utilization",
            "spark_ui_screenshot_path": str(dirs["spark_ui"]),
        }
        (reports_dir / "bottleneck_analysis.json").write_text(json.dumps(bottlenecks, indent=2), encoding="utf-8")

        print("Wrote scaling reports to reports/ directory")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
