#!/usr/bin/env python3
"""Shared Spark pipeline helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import shutil
import yaml
from pyspark.sql import DataFrame, SparkSession


class PipelineError(Exception):
    pass


def load_spark_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def create_spark_session(base_dir: Path, config_path: Path) -> SparkSession:
    cfg = load_spark_config(config_path)
    app_cfg = cfg.get("app", {})
    spark_cfg = cfg.get("spark", {})
    res_cfg = cfg.get("resources", {})

    builder = SparkSession.builder.appName(app_cfg.get("name", "water-bodies-etl")).master(
        app_cfg.get("master", "local[*]")
    )
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    builder = builder.config("spark.sql.adaptive.enabled", str(spark_cfg.get("adaptive_enabled", True)).lower())
    builder = builder.config("spark.sql.shuffle.partitions", str(spark_cfg.get("sql_shuffle_partitions", 200)))
    builder = builder.config("spark.serializer", spark_cfg.get("serializer", "org.apache.spark.serializer.KryoSerializer"))
    builder = builder.config("spark.executor.instances", str(res_cfg.get("executors", 2)))
    builder = builder.config("spark.executor.cores", str(res_cfg.get("cores_per_executor", 2)))
    builder = builder.config("spark.executor.memory", res_cfg.get("executor_memory", "4g"))
    builder = builder.config("spark.ui.enabled", "true")
    builder = builder.config("spark.ui.port", str(spark_cfg.get("ui_port", 4040)))
    builder = builder.config("spark.port.maxRetries", str(spark_cfg.get("ui_port_retries", 20)))
    builder = builder.config("spark.driver.bindAddress", spark_cfg.get("driver_bind_address", "127.0.0.1"))
    builder = builder.config("spark.driver.host", spark_cfg.get("driver_host", "127.0.0.1"))

    spark = builder.getOrCreate()
    spark.sparkContext.setCheckpointDir(str(base_dir / "data" / "processed" / "spark_checkpoints"))
    return spark


def log_lineage(lineage_path: Path, step: str, status: str, details: dict[str, Any]) -> None:
    payload = {
        "step": step,
        "status": status,
        "timestamp_utc": pd.Timestamp.now("UTC").isoformat(),
        "details": details,
    }
    lineage_path.parent.mkdir(parents=True, exist_ok=True)
    with lineage_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def write_csv_single(df: DataFrame, output_csv: Path) -> None:
    temp_dir = output_csv.parent / f".{output_csv.stem}_tmp"
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if output_csv.exists():
        output_csv.unlink()

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(temp_dir))
    part = list(temp_dir.glob("part-*.csv"))
    if not part:
        raise PipelineError(f"No CSV part generated for {output_csv}")
    part[0].rename(output_csv)
    shutil.rmtree(temp_dir)
