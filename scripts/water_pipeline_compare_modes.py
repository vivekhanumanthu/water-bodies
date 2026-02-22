#!/usr/bin/env python3
"""Compare old vs new Spark pipeline approaches for water bodies."""

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

from scripts.pipeline_common import create_spark_session, write_csv_single  # type: ignore
from scripts.run_water_bodies_pipeline import (  # type: ignore
    download_resource,
    ensure_dirs,
    read_with_spark,
    resolve_resource_url,
    validate_ingestion,
)


def _save_plan(df: DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(df._jdf.queryExecution().toString(), encoding="utf-8")  # pylint: disable=protected-access


def _base_enrich(df: DataFrame) -> DataFrame:
    profiled = df.withColumn("ingested_at", F.current_timestamp())
    profiled = profiled.withColumn("ingest_year", F.year("ingested_at")).withColumn("ingest_month", F.month("ingested_at"))

    checks = [F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), 0).otherwise(1) for c in df.columns]
    non_empty = checks[0]
    for expr in checks[1:]:
        non_empty = non_empty + expr
    profiled = profiled.withColumn("non_empty_fields", non_empty)
    profiled = profiled.withColumn("completeness_ratio", F.col("non_empty_fields") / F.lit(float(len(df.columns))))
    profiled = profiled.withColumn(
        "quality_band",
        F.when(F.col("completeness_ratio") >= 0.85, F.lit("HIGH"))
        .when(F.col("completeness_ratio") >= 0.60, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )
    return profiled


def _quality_dim(spark_df: DataFrame) -> DataFrame:
    return spark_df.sparkSession.createDataFrame(
        [("HIGH", 0.85), ("MEDIUM", 0.60), ("LOW", 0.0)],
        ["quality_band", "min_ratio"],
    )


def _query_suite(df: DataFrame, out_dir: Path, use_cache: bool) -> dict[str, float]:
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics: dict[str, float] = {}

    work = df
    if use_cache:
        t0 = time.perf_counter()
        work = work.persist(StorageLevel.MEMORY_AND_DISK)
        _ = work.count()
        metrics["cache_materialize_seconds"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    totals = work.agg(
        F.count(F.lit(1)).alias("total_records"),
        F.avg("completeness_ratio").alias("avg_completeness"),
    )
    write_csv_single(totals, out_dir / "summary_totals.csv")
    metrics["summary_seconds"] = time.perf_counter() - t0

    if "quality_band" in work.columns:
        t0 = time.perf_counter()
        bands = work.groupBy("quality_band").count().withColumnRenamed("count", "records").orderBy(F.desc("records"))
        write_csv_single(bands, out_dir / "quality_band_distribution.csv")
        metrics["quality_band_seconds"] = time.perf_counter() - t0

    if use_cache:
        work.unpersist()

    return metrics


def run_old(df: DataFrame, processed_dir: Path, reports_dir: Path) -> dict[str, float]:
    out_dir = reports_dir / "old"
    t0 = time.perf_counter()

    base = _base_enrich(df)
    # Old approach: regular join (no broadcast), no repartitioning, no cache.
    joined = base.join(_quality_dim(base), on="quality_band", how="left")
    joined.write.mode("overwrite").parquet(str(processed_dir / "curated_old"))
    transform_seconds = time.perf_counter() - t0

    _save_plan(joined, out_dir / "plan_transform.txt")
    query = _query_suite(joined, out_dir, use_cache=False)
    return {"mode": "old", "transform_seconds": transform_seconds, **query}


def run_new(df: DataFrame, processed_dir: Path, reports_dir: Path) -> dict[str, float]:
    out_dir = reports_dir / "new"
    t0 = time.perf_counter()

    base = _base_enrich(df)
    # New approach: broadcast join + repartition by temporal partitions + cache for repeated queries.
    joined = base.join(F.broadcast(_quality_dim(base)), on="quality_band", how="left")
    new_df = joined.repartition("ingest_year", "ingest_month")
    new_df.write.mode("overwrite").partitionBy("ingest_year", "ingest_month").parquet(str(processed_dir / "curated_new"))
    transform_seconds = time.perf_counter() - t0

    _save_plan(new_df, out_dir / "plan_transform.txt")
    query = _query_suite(new_df, out_dir, use_cache=True)
    return {"mode": "new", "transform_seconds": transform_seconds, **query}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare old vs new water-bodies Spark jobs")
    parser.add_argument("--mode", choices=["old", "new", "both"], default="both")
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--config", type=Path, default=Path("config/spark_config.yaml"))
    parser.add_argument("--dataset-url", type=str, default="https://catalog.data.gov/dataset/water-bodies-07739")
    parser.add_argument("--resource-url", type=str, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = args.output_dir.resolve()
    dirs = ensure_dirs(base_dir)

    comp_reports = dirs["reports"] / "comparison"
    comp_reports.mkdir(parents=True, exist_ok=True)

    resource = resolve_resource_url(args.dataset_url, args.resource_url)
    source = download_resource(resource, dirs["raw"])

    spark = create_spark_session(base_dir, args.config)
    try:
        raw = read_with_spark(spark, source, dirs["raw"])
        valid = validate_ingestion(raw, dirs["reports"])

        rows = []
        if args.mode in {"old", "both"}:
            rows.append(run_old(valid, dirs["processed"], comp_reports))
        if args.mode in {"new", "both"}:
            rows.append(run_new(valid, dirs["processed"], comp_reports))

        pd.DataFrame(rows).to_csv(comp_reports / "comparison_runtime.csv", index=False)
        print(f"Comparison reports written to: {comp_reports}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
