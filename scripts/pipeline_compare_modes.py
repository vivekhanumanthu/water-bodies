#!/usr/bin/env python3
"""Run naive vs optimized Spark pipelines on the same task for comparison screenshots."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.run_pipeline import (  # type: ignore
    PipelineError,
    create_spark_session,
    download_dataset,
    ensure_dirs,
    first_existing,
    write_csv_single,
)


def write_plan(df: DataFrame, path: Path) -> None:
    plan_text = df._jdf.queryExecution().toString()  # pylint: disable=protected-access
    path.write_text(plan_text, encoding="utf-8")


def base_transform(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df.where(F.col("crash_record_id").isNotNull() & F.col("crash_date").isNotNull())
        .withColumn("crash_ts", F.to_timestamp(F.col("crash_date")))
        .withColumn("crash_year", F.year(F.col("crash_ts")))
        .withColumn("crash_month", F.month(F.col("crash_ts")))
        .withColumn("crash_ym", F.date_format(F.col("crash_ts"), "yyyy-MM"))
    )


def weather_dim(df: DataFrame) -> DataFrame:
    return df.sparkSession.createDataFrame(
        [
            ("CLEAR", "LOW_RISK"),
            ("RAIN", "MEDIUM_RISK"),
            ("SNOW", "HIGH_RISK"),
            ("FOG/SMOKE/HAZE", "HIGH_RISK"),
            ("UNKNOWN", "UNKNOWN"),
        ],
        ["weather_condition", "weather_risk_band"],
    )


def task_outputs(df: DataFrame, out_dir: Path, persist: bool) -> dict[str, float]:
    out_dir.mkdir(parents=True, exist_ok=True)
    times: dict[str, float] = {}

    working = df
    if persist:
        t0 = time.perf_counter()
        working = working.persist(StorageLevel.MEMORY_AND_DISK)
        _ = working.count()
        times["cache_materialize_seconds"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    totals = working.agg(
        F.count(F.lit(1)).alias("total_crashes"),
        F.sum(F.coalesce(F.col("injuries_total").cast("long"), F.lit(0))).alias("injuries_total"),
        F.sum(F.coalesce(F.col("injuries_fatal").cast("long"), F.lit(0))).alias("injuries_fatal"),
    )
    write_csv_single(totals, out_dir / "summary_totals.csv")
    times["summary_seconds"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    monthly = (
        working.where(F.col("crash_ym").isNotNull())
        .groupBy("crash_ym")
        .count()
        .withColumnRenamed("count", "crashes")
        .orderBy("crash_ym")
    )
    write_csv_single(monthly, out_dir / "crashes_by_month.csv")
    times["monthly_seconds"] = time.perf_counter() - t0

    cause_col = first_existing(working.columns, ["prim_contributory_cause", "prim_cause"])
    if cause_col:
        t0 = time.perf_counter()
        top_causes = (
            working.where(F.col(cause_col).isNotNull() & (F.col(cause_col) != ""))
            .groupBy(F.col(cause_col).alias("cause"))
            .count()
            .withColumnRenamed("count", "crashes")
            .orderBy(F.desc("crashes"))
            .limit(20)
        )
        write_csv_single(top_causes, out_dir / "top_20_primary_causes.csv")
        times["top_causes_seconds"] = time.perf_counter() - t0

    if persist:
        working.unpersist()

    return times


def run_naive(raw_df: DataFrame, reports_root: Path) -> dict[str, float]:
    out_dir = reports_root / "naive"
    clean_dir = reports_root.parent / "data" / "processed" / "crashes_clean_naive"
    clean_dir.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    df = base_transform(raw_df)
    # Naive join: no broadcast hint and no repartitioning.
    joined = df.join(weather_dim(df), on="weather_condition", how="left")
    joined.write.mode("overwrite").parquet(str(clean_dir))
    transform_seconds = time.perf_counter() - t0

    write_plan(joined, out_dir / "plan_transform.txt")

    query_times = task_outputs(joined, out_dir, persist=False)
    return {"mode": "naive", "transform_seconds": transform_seconds, **query_times}


def run_optimized(raw_df: DataFrame, reports_root: Path) -> dict[str, float]:
    out_dir = reports_root / "optimized"
    clean_dir = reports_root.parent / "data" / "processed" / "crashes_clean_optimized"
    clean_dir.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    df = base_transform(raw_df)
    # Optimized join: broadcast tiny dimension + partitioned parquet aligned to temporal queries.
    joined = df.join(F.broadcast(weather_dim(df)), on="weather_condition", how="left")
    partitioned = joined.repartition("crash_year", "crash_month")
    partitioned.write.mode("overwrite").partitionBy("crash_year", "crash_month").parquet(str(clean_dir))
    transform_seconds = time.perf_counter() - t0

    write_plan(partitioned, out_dir / "plan_transform.txt")

    query_times = task_outputs(partitioned, out_dir, persist=True)
    return {"mode": "optimized", "transform_seconds": transform_seconds, **query_times}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare naive vs optimized Spark pipeline")
    parser.add_argument("--mode", choices=["naive", "optimized", "both"], default="both")
    parser.add_argument("--max-rows", type=int, default=150000)
    parser.add_argument("--chunk-size", type=int, default=50000)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--where", type=str, default=None)
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--config", type=Path, default=Path("config/spark_config.yaml"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = args.output_dir.resolve()
    dirs = ensure_dirs(base_dir)

    reports_root = dirs["reports"] / "comparison"
    reports_root.mkdir(parents=True, exist_ok=True)
    (reports_root / "naive").mkdir(parents=True, exist_ok=True)
    (reports_root / "optimized").mkdir(parents=True, exist_ok=True)

    lineage = dirs["reports"] / "comparison_lineage.jsonl"
    if lineage.exists():
        lineage.unlink()

    try:
        json_glob = str(dirs["json_chunks"] / "*.json")
        if not list(dirs["json_chunks"].glob("crashes_chunk_*.json")):
            download_dataset(
                json_chunks_dir=dirs["json_chunks"],
                chunk_size=args.chunk_size,
                max_rows=args.max_rows,
                where_clause=args.where,
                timeout=args.timeout,
                app_token=None,
                lineage_path=lineage,
            )

        spark = create_spark_session(base_dir, args.config)
        try:
            raw_df = spark.read.json(json_glob)
            rows: list[dict[str, float]] = []
            if args.mode in {"naive", "both"}:
                rows.append(run_naive(raw_df, reports_root))
            if args.mode in {"optimized", "both"}:
                rows.append(run_optimized(raw_df, reports_root))

            pd.DataFrame(rows).to_csv(reports_root / "comparison_runtime.csv", index=False)
            print(f"Wrote comparison outputs under: {reports_root}")
            return 0
        finally:
            spark.stop()
    except PipelineError as exc:
        print(f"Comparison failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
