#!/usr/bin/env python3
"""Spark pipeline for Chicago Traffic Crashes with distributed ML and evaluation."""

from __future__ import annotations

import argparse
import json
import os
import pickle
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sns
import yaml
from pyspark import StorageLevel
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.param.shared import Param, Params, TypeConverters
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression as SklearnLogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline as SklearnPipeline
from sklearn.preprocessing import OneHotEncoder as SklearnOneHotEncoder

API_ENDPOINT = "https://data.cityofchicago.org/resource/85ca-t3if.json"
DEFAULT_CHUNK_SIZE = 50_000


class PipelineError(Exception):
    pass


@dataclass
class SplitBounds:
    train_end_epoch: float
    val_end_epoch: float


class RushHourTransformer(
    Transformer,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    """Domain-specific transformer: mark rush-hour crashes (7-9 AM, 4-6 PM)."""

    input_col = Param(Params._dummy(), "input_col", "Input timestamp column", typeConverter=TypeConverters.toString)

    def __init__(self, input_col: str = "crash_ts"):
        super().__init__()
        self._setDefault(input_col="crash_ts")
        self._set(input_col=input_col)

    def get_input_col(self) -> str:
        return self.getOrDefault(self.input_col)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        col = self.get_input_col()
        return (
            dataset.withColumn("crash_hour", F.hour(F.col(col)))
            .withColumn(
                "is_rush_hour",
                F.when((F.col("crash_hour").between(7, 9)) | (F.col("crash_hour").between(16, 18)), F.lit(1)).otherwise(
                    F.lit(0)
                ),
            )
        )


def first_existing(cols: list[str], candidates: list[str]) -> str | None:
    col_set = set(cols)
    for candidate in candidates:
        if candidate in col_set:
            return candidate
    return None


def ensure_dirs(base_dir: Path) -> dict[str, Path]:
    raw_dir = base_dir / "data" / "raw"
    json_chunks_dir = raw_dir / "json_chunks"
    processed_dir = base_dir / "data" / "processed"
    models_dir = base_dir / "data" / "models"
    reports_dir = base_dir / "reports"
    figures_dir = reports_dir / "figures"
    ui_dir = reports_dir / "spark_ui"

    for path in [raw_dir, json_chunks_dir, processed_dir, models_dir, reports_dir, figures_dir, ui_dir]:
        path.mkdir(parents=True, exist_ok=True)

    return {
        "raw": raw_dir,
        "json_chunks": json_chunks_dir,
        "processed": processed_dir,
        "models": models_dir,
        "reports": reports_dir,
        "figures": figures_dir,
        "spark_ui": ui_dir,
    }


def load_spark_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def build_session(app_token: str | None) -> requests.Session:
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    if app_token:
        session.headers["X-App-Token"] = app_token
    return session


def log_lineage(lineage_path: Path, step: str, status: str, details: dict[str, Any]) -> None:
    payload = {
        "step": step,
        "status": status,
        "timestamp_utc": pd.Timestamp.utcnow().isoformat(),
        "details": details,
    }
    with lineage_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def fetch_chunk(
    session: requests.Session,
    offset: int,
    limit: int,
    where_clause: str | None,
    timeout: int,
    max_retries: int = 5,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "$limit": limit,
        "$offset": offset,
        "$order": "crash_date ASC",
    }
    if where_clause:
        params["$where"] = where_clause

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(API_ENDPOINT, params=params, timeout=timeout)
            if response.status_code == 429:
                time.sleep(min(1.5 * attempt, 30))
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            if attempt == max_retries:
                raise PipelineError(f"Failed API request at offset={offset}: {exc}") from exc
            time.sleep(min(1.5 * attempt, 20))

    raise PipelineError("Unexpected API retry failure")


def download_dataset(
    json_chunks_dir: Path,
    chunk_size: int,
    max_rows: int | None,
    where_clause: str | None,
    timeout: int,
    app_token: str | None,
    lineage_path: Path,
) -> tuple[int, int]:
    for existing in json_chunks_dir.glob("crashes_chunk_*.json"):
        existing.unlink()

    session = build_session(app_token)
    offset = 0
    total_rows = 0
    chunk_idx = 0

    log_lineage(lineage_path, "ingestion_start", "ok", {"chunk_size": chunk_size, "max_rows": max_rows})

    while True:
        if max_rows is not None and total_rows >= max_rows:
            break

        limit = chunk_size if max_rows is None else min(chunk_size, max_rows - total_rows)
        rows = fetch_chunk(session, offset, limit, where_clause, timeout)
        if not rows:
            break

        chunk_idx += 1
        output = json_chunks_dir / f"crashes_chunk_{chunk_idx:05d}.json"
        with output.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")

        got = len(rows)
        total_rows += got
        offset += got
        print(f"Downloaded chunk {chunk_idx:05d}: {got:,} rows (total={total_rows:,})")
        if got < limit:
            break

    if total_rows == 0:
        log_lineage(lineage_path, "ingestion_complete", "error", {"reason": "no_rows"})
        raise PipelineError("No rows downloaded from source API")

    log_lineage(lineage_path, "ingestion_complete", "ok", {"total_rows": total_rows, "chunks": chunk_idx})
    return total_rows, chunk_idx


def create_spark_session(base_dir: Path, config_path: Path) -> SparkSession:
    cfg = load_spark_config(config_path)
    app_cfg = cfg.get("app", {})
    spark_cfg = cfg.get("spark", {})
    res_cfg = cfg.get("resources", {})

    builder = SparkSession.builder.appName(app_cfg.get("name", "traffic-crashes-etl")).master(
        app_cfg.get("master", "local[*]")
    )
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    builder = builder.config("spark.sql.adaptive.enabled", str(spark_cfg.get("adaptive_enabled", True)).lower())
    builder = builder.config("spark.sql.shuffle.partitions", str(spark_cfg.get("sql_shuffle_partitions", 200)))
    builder = builder.config("spark.serializer", spark_cfg.get("serializer", "org.apache.spark.serializer.KryoSerializer"))
    builder = builder.config("spark.executor.instances", str(res_cfg.get("executors", 2)))
    builder = builder.config("spark.executor.cores", str(res_cfg.get("cores_per_executor", 2)))
    builder = builder.config("spark.executor.memory", res_cfg.get("executor_memory", "4g"))

    spark = builder.getOrCreate()
    spark.sparkContext.setCheckpointDir(str(base_dir / "data" / "processed" / "spark_checkpoints"))
    return spark


def validate_raw_data(raw_df: DataFrame, reports_dir: Path, lineage_path: Path) -> DataFrame:
    required_cols = ["crash_record_id", "crash_date"]
    missing_cols = [c for c in required_cols if c not in raw_df.columns]
    if missing_cols:
        log_lineage(lineage_path, "validation", "error", {"missing_columns": missing_cols})
        raise PipelineError(f"Missing required columns: {missing_cols}")

    invalid_df = raw_df.where(F.col("crash_record_id").isNull() | F.col("crash_date").isNull())
    valid_df = raw_df.where(F.col("crash_record_id").isNotNull() & F.col("crash_date").isNotNull())

    invalid_count = invalid_df.count()
    total_count = raw_df.count()

    dq_summary = pd.DataFrame(
        [
            {
                "total_rows": total_count,
                "invalid_rows": invalid_count,
                "valid_rows": total_count - invalid_count,
                "invalid_ratio": (invalid_count / total_count) if total_count else 0,
            }
        ]
    )
    dq_summary.to_csv(reports_dir / "data_quality_summary.csv", index=False)
    log_lineage(
        lineage_path,
        "validation",
        "ok",
        {"total_rows": total_count, "invalid_rows": invalid_count, "valid_rows": total_count - invalid_count},
    )
    return valid_df


def create_clean_table(
    spark: SparkSession,
    json_glob: str,
    clean_output_dir: Path,
    reports_dir: Path,
    lineage_path: Path,
) -> DataFrame:
    raw_df = spark.read.json(json_glob)
    valid_df = validate_raw_data(raw_df, reports_dir, lineage_path)

    enriched_df = (
        valid_df.withColumn("crash_ts", F.to_timestamp(F.col("crash_date")))
        .withColumn("crash_year", F.year(F.col("crash_ts")))
        .withColumn("crash_month", F.month(F.col("crash_ts")))
        .withColumn("crash_ym", F.date_format(F.col("crash_ts"), "yyyy-MM"))
    )

    weather_dim = spark.createDataFrame(
        [
            ("CLEAR", "LOW_RISK"),
            ("RAIN", "MEDIUM_RISK"),
            ("SNOW", "HIGH_RISK"),
            ("FOG/SMOKE/HAZE", "HIGH_RISK"),
            ("UNKNOWN", "UNKNOWN"),
        ],
        ["weather_condition", "weather_risk_band"],
    )

    # Broadcast join is appropriate because weather_dim is a tiny lookup table.
    joined_df = enriched_df.join(F.broadcast(weather_dim), on="weather_condition", how="left")

    partitioned_df = joined_df.repartition("crash_year", "crash_month")
    partitioned_df.write.mode("overwrite").partitionBy("crash_year", "crash_month").parquet(str(clean_output_dir))

    log_lineage(
        lineage_path,
        "feature_engineering",
        "ok",
        {
            "output": str(clean_output_dir),
            "partition_cols": ["crash_year", "crash_month"],
            "format": "parquet",
        },
    )
    return partitioned_df


def write_csv_single(df: DataFrame, output_csv: Path) -> None:
    temp_dir = output_csv.parent / f".{output_csv.stem}_tmp"
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


def plot_bar(df: pd.DataFrame, x: str, y: str, title: str, path: Path, rotate_x: bool = False) -> None:
    if df.empty:
        return
    plt.figure(figsize=(12, 6))
    sns.barplot(data=df, x=x, y=y, color="#1f77b4")
    plt.title(title)
    if rotate_x:
        plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def analyze(clean_df: DataFrame, reports_dir: Path, figures_dir: Path, lineage_path: Path) -> None:
    cached = clean_df.persist(StorageLevel.MEMORY_AND_DISK)

    metric_cols = [
        "injuries_total",
        "injuries_fatal",
        "injuries_incapacitating",
        "injuries_non_incapacitating",
        "injuries_reported_not_evident",
        "injuries_no_indication",
    ]
    agg_exprs = [F.count(F.lit(1)).alias("total_crashes")]
    for col in metric_cols:
        if col in cached.columns:
            agg_exprs.append(F.sum(F.coalesce(F.col(col).cast("long"), F.lit(0))).alias(col))

    totals_df = cached.agg(*agg_exprs)
    write_csv_single(totals_df, reports_dir / "summary_totals.csv")

    monthly_df = (
        cached.where(F.col("crash_ym").isNotNull())
        .groupBy("crash_ym")
        .count()
        .withColumnRenamed("count", "crashes")
        .orderBy("crash_ym")
    )
    write_csv_single(monthly_df, reports_dir / "crashes_by_month.csv")

    cause_col = first_existing(cached.columns, ["prim_contributory_cause", "prim_cause"])
    if cause_col:
        top_causes = (
            cached.where(F.col(cause_col).isNotNull() & (F.col(cause_col) != ""))
            .groupBy(F.col(cause_col).alias("cause"))
            .count()
            .withColumnRenamed("count", "crashes")
            .orderBy(F.desc("crashes"))
            .limit(20)
        )
        write_csv_single(top_causes, reports_dir / "top_20_primary_causes.csv")

    monthly_pd = monthly_df.toPandas().tail(24)
    plot_bar(monthly_pd, "crash_ym", "crashes", "Monthly crashes (last 24 months)", figures_dir / "crashes_by_month.png", True)

    cached.unpersist()
    log_lineage(lineage_path, "analytics", "ok", {"outputs": ["summary_totals.csv", "crashes_by_month.csv"]})


def temporal_split(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame, SplitBounds]:
    ts_df = df.withColumn("event_epoch", F.col("crash_ts").cast("long")).where(F.col("event_epoch").isNotNull())
    q70, q85 = ts_df.approxQuantile("event_epoch", [0.70, 0.85], 0.01)
    train_df = ts_df.where(F.col("event_epoch") <= F.lit(q70))
    val_df = ts_df.where((F.col("event_epoch") > F.lit(q70)) & (F.col("event_epoch") <= F.lit(q85)))
    test_df = ts_df.where(F.col("event_epoch") > F.lit(q85))
    return train_df, val_df, test_df, SplitBounds(q70, q85)


def bootstrap_auc_ci(y_true: pd.Series, y_score: pd.Series, n_bootstrap: int = 200) -> tuple[float, float]:
    scores: list[float] = []
    df = pd.DataFrame({"y": y_true, "s": y_score})
    for _ in range(n_bootstrap):
        sampled = df.sample(n=len(df), replace=True)
        if sampled["y"].nunique() < 2:
            continue
        scores.append(roc_auc_score(sampled["y"], sampled["s"]))
    if not scores:
        return (0.0, 0.0)
    scores = sorted(scores)
    return (float(pd.Series(scores).quantile(0.025)), float(pd.Series(scores).quantile(0.975)))


def prepare_model_data(clean_df: DataFrame) -> DataFrame:
    cols = clean_df.columns
    target_col = "injuries_fatal"
    if target_col not in cols:
        raise PipelineError("Column injuries_fatal not found for model target")

    candidate_features = [
        "weather_condition",
        "lighting_condition",
        "roadway_surface_cond",
        "traffic_control_device",
        "first_crash_type",
        "weather_risk_band",
    ]
    feature_cols = [c for c in candidate_features if c in cols]
    if not feature_cols:
        raise PipelineError("No expected feature columns available for modeling")

    return (
        clean_df.select(*(feature_cols + ["crash_ts", target_col]))
        .withColumn("label", (F.coalesce(F.col(target_col).cast("int"), F.lit(0)) > 0).cast("double"))
        .drop(target_col)
        .na.fill("UNKNOWN")
    )


def run_mllib_experiments(
    spark: SparkSession,
    clean_df: DataFrame,
    models_dir: Path,
    reports_dir: Path,
    cv_parallelism: int,
    lineage_path: Path,
) -> None:
    model_df = prepare_model_data(clean_df)
    model_df = RushHourTransformer(input_col="crash_ts").transform(model_df)

    train_df, val_df, test_df, bounds = temporal_split(model_df)
    train_df = train_df.checkpoint(eager=True)

    feature_cols = [
        "weather_condition",
        "lighting_condition",
        "roadway_surface_cond",
        "traffic_control_device",
        "first_crash_type",
        "weather_risk_band",
    ]
    feature_cols = [c for c in feature_cols if c in model_df.columns]
    feature_cols.append("is_rush_hour")

    categorical = [c for c in feature_cols if c != "is_rush_hour"]

    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in categorical]
    encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe", handleInvalid="keep") for c in categorical]

    assembler_inputs = [f"{c}_ohe" for c in categorical] + ["is_rush_hour"]
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features", handleInvalid="keep")

    evaluator_roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    evaluator_pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")

    algorithms = {
        "logistic_regression": (
            LogisticRegression(featuresCol="features", labelCol="label", maxIter=80),
            ParamGridBuilder().addGrid(LogisticRegression.regParam, [0.01, 0.1]).addGrid(
                LogisticRegression.elasticNetParam, [0.0, 0.5]
            ).build(),
        ),
        "random_forest": (
            RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=80),
            ParamGridBuilder().addGrid(RandomForestClassifier.maxDepth, [5, 8]).addGrid(
                RandomForestClassifier.numTrees, [40, 80]
            ).build(),
        ),
        "gbt": (
            GBTClassifier(featuresCol="features", labelCol="label", maxIter=50),
            ParamGridBuilder().addGrid(GBTClassifier.maxDepth, [4, 6]).addGrid(GBTClassifier.maxIter, [30, 50]).build(),
        ),
    }

    metrics_rows: list[dict[str, Any]] = []

    for name, (algo, grid) in algorithms.items():
        pipeline = Pipeline(stages=indexers + encoders + [assembler, algo])
        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=grid,
            evaluator=evaluator_roc,
            numFolds=3,
            parallelism=cv_parallelism,
        )
        start = time.perf_counter()
        cv_model = cv.fit(train_df)
        train_time = time.perf_counter() - start

        best_model = cv_model.bestModel
        model_path = models_dir / f"{name}_spark_model"
        best_model.write().overwrite().save(str(model_path))

        val_pred = best_model.transform(val_df)
        test_pred = best_model.transform(test_df)
        val_roc = evaluator_roc.evaluate(val_pred)
        test_roc = evaluator_roc.evaluate(test_pred)
        test_pr = evaluator_pr.evaluate(test_pred)

        pred_pd = test_pred.select(F.col("label").cast("int").alias("label"), F.col("probability")[1].alias("score")).toPandas()
        ci_low, ci_high = bootstrap_auc_ci(pred_pd["label"], pred_pd["score"], n_bootstrap=200)

        expected_cost_avoided = float(pred_pd["score"].mean() * 1000.0)

        metrics_rows.append(
            {
                "model": name,
                "val_roc_auc": float(val_roc),
                "test_roc_auc": float(test_roc),
                "test_pr_auc": float(test_pr),
                "auc_ci_low": ci_low,
                "auc_ci_high": ci_high,
                "train_seconds": train_time,
                "expected_cost_avoided_index": expected_cost_avoided,
            }
        )

    metrics_df = pd.DataFrame(metrics_rows)
    metrics_df.to_csv(reports_dir / "mllib_model_comparison.csv", index=False)

    log_lineage(
        lineage_path,
        "mllib_training",
        "ok",
        {
            "models": list(algorithms.keys()),
            "temporal_split": {
                "train_end_epoch": bounds.train_end_epoch,
                "val_end_epoch": bounds.val_end_epoch,
            },
        },
    )


def run_sklearn_baseline(clean_df: DataFrame, models_dir: Path, reports_dir: Path) -> None:
    feature_cols = [
        c
        for c in [
            "weather_condition",
            "lighting_condition",
            "roadway_surface_cond",
            "traffic_control_device",
            "first_crash_type",
        ]
        if c in clean_df.columns
    ]
    if "injuries_fatal" not in clean_df.columns or not feature_cols:
        return

    sample_pd = (
        clean_df.select(*(feature_cols + ["injuries_fatal"]))
        .withColumn("label", (F.coalesce(F.col("injuries_fatal").cast("int"), F.lit(0)) > 0).cast("int"))
        .drop("injuries_fatal")
        .limit(150_000)
        .toPandas()
    )
    if sample_pd.empty or sample_pd["label"].nunique() < 2:
        return

    X = sample_pd[feature_cols]
    y = sample_pd["label"]

    pre = ColumnTransformer(
        transformers=[("cat", SklearnOneHotEncoder(handle_unknown="ignore"), feature_cols)],
        remainder="drop",
    )
    model = SklearnPipeline(
        steps=[
            ("pre", pre),
            ("clf", SklearnLogisticRegression(max_iter=400, class_weight="balanced")),
        ]
    )
    model.fit(X, y)
    y_score = model.predict_proba(X)[:, 1]
    auc = roc_auc_score(y, y_score)

    with (models_dir / "sklearn_logistic.pkl").open("wb") as handle:
        pickle.dump(model, handle)

    pd.DataFrame([{"model": "sklearn_logistic", "roc_auc_train_sample": float(auc), "rows": len(sample_pd)}]).to_csv(
        reports_dir / "sklearn_baseline.csv", index=False
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Spark data engineering + distributed ML pipeline")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--where", type=str, default=None)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--config", type=Path, default=Path("config/spark_config.yaml"))
    parser.add_argument("--skip-ml", action="store_true", help="Skip ML stage")
    parser.add_argument("--cv-parallelism", type=int, default=2)
    return parser


def main() -> int:
    args = build_argument_parser().parse_args()
    app_token = os.getenv("SOCRATA_APP_TOKEN")
    base_dir = args.output_dir.resolve()
    dirs = ensure_dirs(base_dir)
    lineage_path = dirs["reports"] / "pipeline_lineage.jsonl"
    if lineage_path.exists():
        lineage_path.unlink()

    spark: SparkSession | None = None

    try:
        download_dataset(
            json_chunks_dir=dirs["json_chunks"],
            chunk_size=args.chunk_size,
            max_rows=args.max_rows,
            where_clause=args.where,
            timeout=args.timeout,
            app_token=app_token,
            lineage_path=lineage_path,
        )

        spark = create_spark_session(base_dir, args.config)
        clean_df = create_clean_table(
            spark=spark,
            json_glob=str(dirs["json_chunks"] / "*.json"),
            clean_output_dir=dirs["processed"] / "crashes_clean",
            reports_dir=dirs["reports"],
            lineage_path=lineage_path,
        )

        analyze(clean_df, dirs["reports"], dirs["figures"], lineage_path)

        if not args.skip_ml:
            run_mllib_experiments(
                spark=spark,
                clean_df=clean_df,
                models_dir=dirs["models"],
                reports_dir=dirs["reports"],
                cv_parallelism=args.cv_parallelism,
                lineage_path=lineage_path,
            )
            run_sklearn_baseline(clean_df, dirs["models"], dirs["reports"])

        log_lineage(lineage_path, "pipeline_complete", "ok", {"skip_ml": args.skip_ml})
        print("Pipeline completed successfully.")
        return 0
    except Exception as exc:  # pylint: disable=broad-except
        log_lineage(lineage_path, "pipeline_complete", "error", {"message": str(exc)})
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
