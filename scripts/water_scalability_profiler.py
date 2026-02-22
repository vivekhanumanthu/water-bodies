#!/usr/bin/env python3
"""Strong/weak scaling profiler for water-bodies pipeline outputs."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.pipeline_common import create_spark_session
from scripts.run_water_bodies_pipeline import ensure_dirs


def timed_query(df) -> float:
    start = time.perf_counter()
    _ = df.groupBy("ingest_year").agg(F.count("*").alias("rows"), F.avg("completeness_ratio").alias("avg_comp")).collect()
    return time.perf_counter() - start


def strong_scaling(df, partitions: list[int], fixed_rows: int) -> pd.DataFrame:
    base = df.limit(fixed_rows)
    rows = []
    for p in partitions:
        candidate = base.repartition(p).cache()
        t = timed_query(candidate)
        rows.append({"mode": "strong", "rows": fixed_rows, "partitions": p, "seconds": t})
        candidate.unpersist()
    return pd.DataFrame(rows)


def weak_scaling(df, partitions: list[int], rows_per_partition: int) -> pd.DataFrame:
    rows = []
    for p in partitions:
        n = p * rows_per_partition
        candidate = df.limit(n).repartition(p).cache()
        t = timed_query(candidate)
        rows.append({"mode": "weak", "rows": n, "partitions": p, "seconds": t})
        candidate.unpersist()
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run water-bodies scaling experiments")
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--config", type=Path, default=Path("config/spark_config.yaml"))
    parser.add_argument("--partitions", type=str, default="2,4,8")
    parser.add_argument("--fixed-rows", type=int, default=100000)
    parser.add_argument("--rows-per-partition", type=int, default=30000)
    parser.add_argument("--cost-per-core-hour", type=float, default=0.06)
    args = parser.parse_args()

    base_dir = args.output_dir.resolve()
    dirs = ensure_dirs(base_dir)
    reports = dirs["reports"]

    spark = create_spark_session(base_dir, args.config)
    try:
        curated = spark.read.parquet(str(dirs["processed"] / "curated"))
        parts = [int(x.strip()) for x in args.partitions.split(",") if x.strip()]

        strong_df = strong_scaling(curated, parts, args.fixed_rows)
        weak_df = weak_scaling(curated, parts, args.rows_per_partition)

        strong_df.to_csv(reports / "scaling_strong.csv", index=False)
        weak_df.to_csv(reports / "scaling_weak.csv", index=False)

        merged = pd.concat([strong_df, weak_df], ignore_index=True)
        merged["rows_per_second"] = merged["rows"] / merged["seconds"]
        merged["core_hours"] = (merged["seconds"] / 3600.0) * merged["partitions"]
        merged["estimated_cost"] = merged["core_hours"] * args.cost_per_core_hour
        merged.to_csv(reports / "scaling_cost_performance.csv", index=False)

        bottleneck = {
            "likely_io_bound_when": "rows_per_second plateaus as partitions rise",
            "likely_shuffle_bound_when": "strong-scaling speedup flattens after higher partition counts",
            "likely_compute_bound_when": "executor CPU saturation occurs with little throughput gain",
            "spark_ui_screenshot_path": str(dirs["spark_ui"]),
        }
        (reports / "bottleneck_analysis.json").write_text(json.dumps(bottleneck, indent=2), encoding="utf-8")
        print(f"Scaling reports written to {reports}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
