#!/usr/bin/env python3
"""Comprehensive Spark pipeline for Data.gov Water Bodies dataset(s)."""

from __future__ import annotations

import argparse
import csv
import json
import pickle
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
from pyspark import StorageLevel
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.param.shared import Param, Params, TypeConverters
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression as SkLogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.preprocessing import OneHotEncoder as SkOneHotEncoder

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.pipeline_common import (  # type: ignore
    PipelineError,
    create_spark_session,
    load_spark_config,
    log_lineage,
    write_csv_single,
)

DEFAULT_DATASET_URL = "https://catalog.data.gov/dataset/water-bodies-07739"


class WaterProfileTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    """Domain transformer that derives row-level completeness and text richness features."""

    columns_json = Param(Params._dummy(), "columns_json", "JSON list of source columns", TypeConverters.toString)

    def __init__(self, columns: list[str]):
        super().__init__()
        self._set(columns_json=json.dumps(columns))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        cols = json.loads(self.getOrDefault(self.columns_json))
        if not cols:
            return dataset.withColumn("completeness_ratio", F.lit(1.0)).withColumn("non_empty_fields", F.lit(0))

        checks = [F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), 0).otherwise(1) for c in cols]
        non_empty = checks[0]
        for expr in checks[1:]:
            non_empty = non_empty + expr

        out = dataset.withColumn("non_empty_fields", non_empty)
        out = out.withColumn("completeness_ratio", F.col("non_empty_fields") / F.lit(float(len(cols))))

        name_col = next((c for c in cols if "name" in c.lower()), None)
        if name_col:
            out = out.withColumn("name_length", F.length(F.coalesce(F.col(name_col).cast("string"), F.lit(""))))
        else:
            out = out.withColumn("name_length", F.lit(0))

        return out


def ensure_dirs(base_dir: Path) -> dict[str, Path]:
    raw_dir = base_dir / "data" / "raw" / "water_bodies"
    processed_dir = base_dir / "data" / "processed" / "water_bodies"
    models_dir = base_dir / "data" / "models" / "water_bodies"
    reports_dir = base_dir / "reports" / "water_bodies"
    figures_dir = reports_dir / "figures"
    ui_dir = reports_dir / "spark_ui"

    for path in [raw_dir, processed_dir, models_dir, reports_dir, figures_dir, ui_dir]:
        path.mkdir(parents=True, exist_ok=True)

    return {
        "raw": raw_dir,
        "processed": processed_dir,
        "models": models_dir,
        "reports": reports_dir,
        "figures": figures_dir,
        "spark_ui": ui_dir,
    }


def _resource_rank(resource: dict[str, Any]) -> int:
    fmt = str(resource.get("format", "")).lower()
    url = str(resource.get("url", "")).lower()
    if "csv" in fmt or url.endswith(".csv"):
        return 0
    if "geojson" in fmt or "geojson" in url:
        return 1
    if "json" in fmt or url.endswith(".json"):
        return 2
    return 9


def resolve_resource_url(dataset_url: str, resource_url: str | None) -> str:
    if resource_url:
        return resource_url

    lowered = dataset_url.lower()
    if lowered.endswith((".csv", ".json", ".geojson")):
        return dataset_url

    parts = [p for p in urlparse(dataset_url).path.split("/") if p]
    if "dataset" not in parts:
        raise PipelineError("Dataset URL is not a Data.gov dataset page. Use --resource-url.")

    slug = parts[-1]
    resp = requests.get("https://catalog.data.gov/api/3/action/package_show", params={"id": slug}, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    resources = payload.get("result", {}).get("resources", []) if payload.get("success") else []
    if not resources:
        raise PipelineError(f"No resources listed for dataset slug={slug}")

    url = sorted(resources, key=_resource_rank)[0].get("url")
    if not url:
        raise PipelineError("Resolved resource has no URL")
    return str(url)


def download_resource(resource_url: str, raw_dir: Path) -> Path:
    resp = requests.get(resource_url, timeout=120)
    resp.raise_for_status()
    content = resp.content
    content_type = resp.headers.get("content-type", "").lower()

    sample = content[:4096].decode("utf-8", errors="ignore").lstrip()
    first_line = sample.splitlines()[0] if sample.splitlines() else ""

    ext = ".json"
    lowered = resource_url.lower()
    if lowered.endswith(".csv") or "text/csv" in content_type:
        ext = ".csv"
    elif lowered.endswith(".geojson") or "geo+json" in content_type or ("FeatureCollection" in sample and "\"type\"" in sample):
        ext = ".geojson"
    elif lowered.endswith(".json") or "application/json" in content_type:
        ext = ".json"
    elif "," in first_line and not first_line.startswith(("{", "[")):
        ext = ".csv"

    path = raw_dir / f"water_bodies_source{ext}"
    path.write_bytes(content)
    return path


def _normalize_geojson(source_path: Path, normalized_path: Path) -> None:
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    features = payload.get("features", [])
    with normalized_path.open("w", encoding="utf-8") as handle:
        for feat in features:
            props = feat.get("properties", {}) or {}
            geom = feat.get("geometry", {}) or {}
            props["geometry_type"] = geom.get("type")
            handle.write(json.dumps(props) + "\n")


def read_with_spark(spark: SparkSession, source_path: Path, raw_dir: Path) -> DataFrame:
    suffix = source_path.suffix.lower()
    if suffix == ".csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(str(source_path))

    if suffix == ".geojson":
        normalized = raw_dir / "water_bodies_normalized.jsonl"
        _normalize_geojson(source_path, normalized)
        return spark.read.json(str(normalized))

    try:
        parsed = spark.read.json(str(source_path))
        if parsed.columns == ["_corrupt_record"] or not parsed.columns:
            raise PipelineError("Spark JSON parse returned only _corrupt_record")
        return parsed
    except Exception:  # pylint: disable=broad-except
        text = source_path.read_text(encoding="utf-8", errors="ignore").lstrip()
        if not text:
            raise PipelineError("Downloaded source is empty")

        if text.startswith(("{", "[")):
            payload = json.loads(text)
            if isinstance(payload, dict) and isinstance(payload.get("features"), list):
                tmp_geo = raw_dir / "water_bodies_fallback.geojson"
                tmp_geo.write_text(text, encoding="utf-8")
                normalized = raw_dir / "water_bodies_normalized.jsonl"
                _normalize_geojson(tmp_geo, normalized)
                return spark.read.json(str(normalized))

            if isinstance(payload, list):
                rows = payload
            elif isinstance(payload, dict) and isinstance(payload.get("results"), list):
                rows = payload["results"]
            else:
                raise PipelineError("Unsupported JSON structure for water bodies source")

            normalized = raw_dir / "water_bodies_normalized.jsonl"
            with normalized.open("w", encoding="utf-8") as h:
                for row in rows:
                    h.write(json.dumps(row) + "\n")
            return spark.read.json(str(normalized))

        first = text.splitlines()[0] if text.splitlines() else ""
        if "," in first:
            return spark.read.option("header", True).option("inferSchema", True).csv(str(source_path))

        raise PipelineError("Unable to infer source format. Use --water-resource-url with direct CSV/GeoJSON URL.")


def validate_ingestion(df: DataFrame, reports_dir: Path) -> DataFrame:
    total = df.count()
    if total == 0:
        raise PipelineError("Input dataset is empty")

    no_cols = len(df.columns)
    if no_cols == 0:
        raise PipelineError("Input dataset has no columns")

    miss_cols = [
        F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), F.lit(1)).otherwise(F.lit(0)).alias(c)
        for c in df.columns
    ]
    miss_df = df.select(*miss_cols)
    miss_expr = F.lit(0)
    for c in df.columns:
        miss_expr = miss_expr + F.col(c)
    empty_rows = miss_df.select(miss_expr.alias("miss")).where(F.col("miss") == F.lit(len(df.columns))).count()

    pd.DataFrame(
        [{"total_rows": total, "total_columns": no_cols, "completely_empty_rows": empty_rows}]
    ).to_csv(reports_dir / "ingestion_validation.csv", index=False)

    return df


def apply_processing_pipeline(df: DataFrame, processed_dir: Path) -> DataFrame:
    profiled = WaterProfileTransformer(columns=df.columns).transform(df)
    profiled = profiled.withColumn("ingested_at", F.current_timestamp())
    profiled = profiled.withColumn("ingest_year", F.year("ingested_at")).withColumn("ingest_month", F.month("ingested_at"))

    # Broadcast join with tiny quality dimension to avoid shuffle-heavy join.
    qdim = df.sparkSession.createDataFrame(
        [("HIGH", 0.85), ("MEDIUM", 0.60), ("LOW", 0.0)],
        ["quality_band", "min_ratio"],
    )

    scored = profiled.withColumn(
        "quality_band",
        F.when(F.col("completeness_ratio") >= 0.85, F.lit("HIGH"))
        .when(F.col("completeness_ratio") >= 0.60, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )

    joined = scored.join(F.broadcast(qdim), on="quality_band", how="left")
    curated = joined.repartition("ingest_year", "ingest_month")
    curated.write.mode("overwrite").partitionBy("ingest_year", "ingest_month").parquet(str(processed_dir / "curated"))
    return curated


def compute_null_profile(df: DataFrame, out_csv: Path) -> None:
    total = df.count()
    agg_exprs = [
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]
    null_row = df.agg(*agg_exprs).collect()[0].asDict()
    rows = [{"column": c, "null_count": int(null_row.get(c, 0)), "null_ratio": (float(null_row.get(c, 0)) / total) if total else 0.0} for c in df.columns]
    pd.DataFrame(rows).sort_values("null_ratio", ascending=False).to_csv(out_csv, index=False)


def compute_column_inventory(df: DataFrame, out_csv: Path) -> None:
    total = df.count()
    rows: list[dict[str, Any]] = []
    for c, t in df.dtypes:
        s = df.select(F.count(F.col(c)).alias("nn"), F.approx_count_distinct(F.col(c)).alias("dc")).collect()[0]
        non_null = int(s["nn"])
        rows.append(
            {
                "column": c,
                "data_type": t,
                "non_null_count": non_null,
                "null_count": total - non_null,
                "fill_rate": (non_null / total) if total else 0.0,
                "distinct_count": int(s["dc"]),
            }
        )
    pd.DataFrame(rows).sort_values(["fill_rate", "distinct_count"], ascending=[False, False]).to_csv(out_csv, index=False)


def compute_numeric_statistics(df: DataFrame, out_csv: Path) -> None:
    numeric_types = {"tinyint", "smallint", "int", "bigint", "float", "double", "decimal", "long"}
    numeric_cols = [c for c, t in df.dtypes if any(t.startswith(n) for n in numeric_types)]
    if not numeric_cols:
        pd.DataFrame([{"note": "No numeric columns detected"}]).to_csv(out_csv, index=False)
        return

    rows = []
    for c in numeric_cols[:60]:
        q = df.select(
            F.mean(F.col(c)).alias("mean"),
            F.stddev(F.col(c)).alias("stddev"),
            F.min(F.col(c)).alias("min"),
            F.expr(f"percentile_approx({c}, 0.5)").alias("median"),
            F.max(F.col(c)).alias("max"),
        ).collect()[0]
        rows.append({"column": c, "mean": q["mean"], "stddev": q["stddev"], "min": q["min"], "median": q["median"], "max": q["max"]})
    pd.DataFrame(rows).to_csv(out_csv, index=False)


def compute_top_values(df: DataFrame, out_csv: Path) -> None:
    candidates = [c for c, t in df.dtypes if t == "string" and c not in {"ingested_at"}]
    rows: list[dict[str, Any]] = []
    for c in candidates[:20]:
        top = (
            df.where(F.col(c).isNotNull() & (F.trim(F.col(c)) != ""))
            .groupBy(c)
            .count()
            .orderBy(F.desc("count"))
            .limit(5)
            .collect()
        )
        for r in top:
            rows.append({"column": c, "value": str(r[c]), "count": int(r["count"])})
    if rows:
        pd.DataFrame(rows).to_csv(out_csv, index=False)
    else:
        pd.DataFrame([{"note": "No string columns available"}]).to_csv(out_csv, index=False)


def bootstrap_auc_ci(y_true: pd.Series, y_score: pd.Series, n_bootstrap: int = 200) -> tuple[float, float]:
    vals: list[float] = []
    base = pd.DataFrame({"y": y_true, "s": y_score})
    for _ in range(n_bootstrap):
        sampled = base.sample(n=len(base), replace=True)
        if sampled["y"].nunique() < 2:
            continue
        vals.append(roc_auc_score(sampled["y"], sampled["s"]))
    if not vals:
        return (0.0, 0.0)
    vals = sorted(vals)
    return (float(pd.Series(vals).quantile(0.025)), float(pd.Series(vals).quantile(0.975)))


def prepare_ml_data(curated_df: DataFrame) -> tuple[DataFrame, list[str], list[str]]:
    leakage_cols = {
        "ingested_at",
        "ingest_year",
        "ingest_month",
        "quality_band",
        "min_ratio",
        "completeness_ratio",
        "non_empty_fields",
        "label",
        "event_order",
    }

    # Build label from a dedicated subset of fields and block those fields from model features.
    base_cols = [c for c in curated_df.columns if c not in leakage_cols]
    label_basis_candidates = [c for c in base_cols if not c.lower().endswith("id")]
    if not label_basis_candidates:
        label_basis_candidates = base_cols
    label_basis_count = max(1, min(4, len(label_basis_candidates) // 2 or 1))
    label_basis_cols = sorted(label_basis_candidates)[:label_basis_count]

    checks = [
        F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), F.lit(0.0)).otherwise(F.lit(1.0))
        for c in label_basis_cols
    ]
    label_non_empty = checks[0]
    for expr in checks[1:]:
        label_non_empty = label_non_empty + expr

    labeled = curated_df.withColumn("label_basis_ratio", label_non_empty / F.lit(float(len(label_basis_cols))))
    q = labeled.approxQuantile("label_basis_ratio", [0.15], 0.01)
    threshold = q[0] if q else 0.6
    labeled = labeled.withColumn("label", F.when(F.col("label_basis_ratio") <= F.lit(threshold), 1.0).otherwise(0.0))
    labeled = labeled.withColumn("event_order", F.monotonically_increasing_id())
    labeled = labeled.drop("label_basis_ratio")

    feature_candidates = [c for c in curated_df.columns if c not in leakage_cols and c not in set(label_basis_cols)]
    feature_candidates = [c for c in feature_candidates if "quality" not in c.lower() and "completeness" not in c.lower()]
    feature_candidates = [c for c in feature_candidates if not c.lower().endswith("id")]

    # Leakage guard: keep columns that are mostly non-missing so label-by-missingness is harder to memorize.
    if feature_candidates:
        miss_exprs = [
            F.avg(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), F.lit(1.0)).otherwise(F.lit(0.0))).alias(c)
            for c in feature_candidates
        ]
        miss_ratios = labeled.select(*miss_exprs).collect()[0].asDict()
        strict = [c for c in feature_candidates if float(miss_ratios.get(c, 1.0)) <= 0.10]
        relaxed = [c for c in feature_candidates if float(miss_ratios.get(c, 1.0)) <= 0.30]
        feature_candidates = strict if strict else relaxed

    numeric_types = {"tinyint", "smallint", "int", "bigint", "float", "double", "decimal", "long"}
    numeric_cols = [c for c, t in labeled.dtypes if c in feature_candidates and any(t.startswith(n) for n in numeric_types)]
    cat_cols = [c for c, t in labeled.dtypes if c in feature_candidates and t == "string"]

    numeric_cols = [c for c in numeric_cols if c not in {"label"}][:8]
    cat_cols = cat_cols[:8]

    if not numeric_cols and not cat_cols:
        raise PipelineError("No usable columns for ML features")

    selected = list(dict.fromkeys(cat_cols + numeric_cols + ["label", "event_order"]))
    return labeled.select(*selected).na.fill("UNKNOWN"), cat_cols, numeric_cols


def stratified_temporal_split(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    win = Window.partitionBy("label").orderBy("event_order")
    with_rank = df.withColumn("pct_in_label", F.percent_rank().over(win))

    train = with_rank.where(F.col("pct_in_label") <= 0.70)
    val = with_rank.where((F.col("pct_in_label") > 0.70) & (F.col("pct_in_label") <= 0.85))
    test = with_rank.where(F.col("pct_in_label") > 0.85)
    return train.drop("pct_in_label"), val.drop("pct_in_label"), test.drop("pct_in_label")


def run_mllib(curated_df: DataFrame, models_dir: Path, reports_dir: Path, cv_parallelism: int) -> None:
    ml_df, cat_cols, numeric_cols = prepare_ml_data(curated_df)
    train_df, val_df, test_df = stratified_temporal_split(ml_df)

    train_df = train_df.checkpoint(eager=True)

    split_dist = []
    for split_name, split_df in [("train", train_df), ("validation", val_df), ("test", test_df)]:
        counts = split_df.groupBy("label").count().collect()
        total = sum(int(r["count"]) for r in counts) or 1
        for r in counts:
            split_dist.append(
                {
                    "split": split_name,
                    "label": int(r["label"]),
                    "count": int(r["count"]),
                    "ratio": float(r["count"]) / total,
                }
            )
    pd.DataFrame(split_dist).to_csv(reports_dir / "split_class_distribution.csv", index=False)

    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
    encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe", handleInvalid="keep") for c in cat_cols]
    assembler_inputs = [f"{c}_ohe" for c in cat_cols] + numeric_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features", handleInvalid="keep")

    evaluator_roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    evaluator_pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")

    algos = {
        "logistic_regression": (
            LogisticRegression(featuresCol="features", labelCol="label", maxIter=80),
            ParamGridBuilder().addGrid(LogisticRegression.regParam, [0.01, 0.1]).addGrid(
                LogisticRegression.elasticNetParam, [0.0, 0.5]
            ).build(),
        ),
        "random_forest": (
            RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=80),
            ParamGridBuilder().addGrid(RandomForestClassifier.maxDepth, [4, 8]).addGrid(
                RandomForestClassifier.numTrees, [40, 80]
            ).build(),
        ),
        "gbt": (
            GBTClassifier(featuresCol="features", labelCol="label", maxIter=40),
            ParamGridBuilder().addGrid(GBTClassifier.maxDepth, [3, 6]).addGrid(GBTClassifier.maxIter, [20, 40]).build(),
        ),
    }

    rows: list[dict[str, Any]] = []
    import_rows: list[dict[str, Any]] = []

    for name, (algo, grid) in algos.items():
        pipe = Pipeline(stages=indexers + encoders + [assembler, algo])
        cv = CrossValidator(
            estimator=pipe,
            estimatorParamMaps=grid,
            evaluator=evaluator_roc,
            numFolds=3,
            parallelism=cv_parallelism,
        )

        start = time.perf_counter()
        cv_model = cv.fit(train_df)
        train_sec = time.perf_counter() - start
        best = cv_model.bestModel

        best.write().overwrite().save(str(models_dir / f"{name}_spark_model"))

        val_pred = best.transform(val_df)
        test_pred = best.transform(test_df)

        val_auc = evaluator_roc.evaluate(val_pred)
        test_auc = evaluator_roc.evaluate(test_pred)
        test_pr = evaluator_pr.evaluate(test_pred)

        pred_pd = test_pred.select(F.col("label").cast("int").alias("label"), vector_to_array(F.col("probability"))[1].alias("score")).toPandas()
        ci_low, ci_high = bootstrap_auc_ci(pred_pd["label"], pred_pd["score"], n_bootstrap=200)

        pred_pd["pred"] = (pred_pd["score"] >= 0.5).astype(int)
        tp = int(((pred_pd["pred"] == 1) & (pred_pd["label"] == 1)).sum())
        fp = int(((pred_pd["pred"] == 1) & (pred_pd["label"] == 0)).sum())
        fn = int(((pred_pd["pred"] == 0) & (pred_pd["label"] == 1)).sum())
        expected_profit = tp * 600 - fp * 120 - fn * 350

        rows.append(
            {
                "model": name,
                "val_roc_auc": float(val_auc),
                "test_roc_auc": float(test_auc),
                "test_pr_auc": float(test_pr),
                "auc_ci_low": ci_low,
                "auc_ci_high": ci_high,
                "train_seconds": train_sec,
                "expected_profit_index": float(expected_profit),
                "test_rows": len(pred_pd),
            }
        )

        final_stage = best.stages[-1]
        if hasattr(final_stage, "featureImportances"):
            imp = list(final_stage.featureImportances)
            for i, v in enumerate(imp[:60]):
                import_rows.append({"model": name, "feature_index": i, "importance": float(v)})

    pd.DataFrame(rows).to_csv(reports_dir / "mllib_model_comparison.csv", index=False)
    if import_rows:
        pd.DataFrame(import_rows).to_csv(reports_dir / "feature_importance_index.csv", index=False)

    run_sklearn_baseline(train_df, test_df, cat_cols, numeric_cols, models_dir, reports_dir)


def run_sklearn_baseline(
    train_df: DataFrame,
    test_df: DataFrame,
    cat_cols: list[str],
    numeric_cols: list[str],
    models_dir: Path,
    reports_dir: Path,
) -> None:
    selected_cols = cat_cols + numeric_cols + ["label"]
    train_pd = train_df.select(*selected_cols).limit(120_000).toPandas()
    test_pd = test_df.select(*selected_cols).limit(120_000).toPandas()
    if train_pd.empty or test_pd.empty:
        return
    if train_pd["label"].nunique() < 2 or test_pd["label"].nunique() < 2:
        return

    X_train = train_pd[cat_cols + numeric_cols]
    y_train = train_pd["label"].astype(int)
    X_test = test_pd[cat_cols + numeric_cols]
    y_test = test_pd["label"].astype(int)

    pre = ColumnTransformer(
        transformers=[
            ("cat", SkOneHotEncoder(handle_unknown="ignore"), cat_cols),
            ("num", "passthrough", numeric_cols),
        ]
    )
    model = SkPipeline(steps=[("pre", pre), ("clf", SkLogisticRegression(max_iter=400, class_weight="balanced"))])
    model.fit(X_train, y_train)

    train_score = model.predict_proba(X_train)[:, 1]
    test_score = model.predict_proba(X_test)[:, 1]
    auc_train = roc_auc_score(y_train, train_score)
    auc_test = roc_auc_score(y_test, test_score)

    with (models_dir / "sklearn_baseline.pkl").open("wb") as h:
        pickle.dump(model, h)

    pd.DataFrame(
        [
            {
                "model": "sklearn_logistic",
                "roc_auc_train_sample": float(auc_train),
                "roc_auc_test_sample": float(auc_test),
                "train_rows": len(train_pd),
                "test_rows": len(test_pd),
            }
        ]
    ).to_csv(
        reports_dir / "sklearn_baseline.csv", index=False
    )


def write_executive_summary(reports_dir: Path, total_rows: int, total_columns: int) -> None:
    null_profile = pd.read_csv(reports_dir / "null_profile_top20.csv")
    col_inventory = pd.read_csv(reports_dir / "column_inventory.csv")
    most_missing = null_profile.sort_values("null_ratio", ascending=False).head(5)
    strongest = col_inventory.sort_values("fill_rate", ascending=False).head(5)

    lines = [
        "# Water Bodies Data Profiling Executive Summary",
        "",
        f"- Total records processed: **{total_rows:,}**",
        f"- Total columns profiled: **{total_columns}**",
        "",
        "## Key Data Quality Findings",
        "Top columns with highest missingness:",
    ]
    for _, r in most_missing.iterrows():
        lines.append(f"- `{r['column']}`: null ratio {r['null_ratio']:.2%} ({int(r['null_count']):,} nulls)")

    lines.append("")
    lines.append("Most complete columns:")
    for _, r in strongest.iterrows():
        lines.append(f"- `{r['column']}`: fill rate {r['fill_rate']:.2%}, distinct values {int(r['distinct_count']):,}")

    if (reports_dir / "mllib_model_comparison.csv").exists():
        m = pd.read_csv(reports_dir / "mllib_model_comparison.csv").sort_values("test_roc_auc", ascending=False).head(1)
        if not m.empty:
            lines.append("")
            lines.append("## Model Evaluation Summary")
            lines.append(f"- Best model: `{m.iloc[0]['model']}` with test ROC-AUC `{m.iloc[0]['test_roc_auc']:.4f}`")
            lines.append(
                f"- 95% bootstrap CI: [{m.iloc[0]['auc_ci_low']:.4f}, {m.iloc[0]['auc_ci_high']:.4f}]"
            )
            lines.append(f"- Expected profit index: `{m.iloc[0]['expected_profit_index']:.2f}`")

    lines += [
        "",
        "## Engineering and Optimization Notes",
        "- DataFrame APIs are used throughout for Catalyst optimization and Tungsten execution.",
        "- Broadcast join applied for tiny quality dimension.",
        "- MEMORY_AND_DISK caching used in report stage; explicitly unpersisted after use.",
        "- Partitioning strategy: Parquet partition by ingest_year/ingest_month.",
        "- Spark UI screenshots should be saved under reports/water_bodies/spark_ui/.",
    ]

    (reports_dir / "executive_summary.md").write_text("\n".join(lines), encoding="utf-8")


def write_engineering_notes(reports_dir: Path, cfg: dict[str, Any]) -> None:
    notes = [
        "# Engineering Notes",
        "",
        "## DataFrame vs RDD Justification",
        "DataFrame API is used to enable Catalyst optimization, predicate pushdown, and optimized physical planning.",
        "RDD is avoided for core ETL/ML tasks because it loses schema-level optimization benefits.",
        "",
        "## Caching Strategy",
        "Curated dataframe is persisted with MEMORY_AND_DISK during repeated report queries and unpersisted immediately after exports.",
        "",
        "## Shuffle and Partition Tuning",
        f"spark.sql.shuffle.partitions = {cfg.get('spark', {}).get('sql_shuffle_partitions', 'default')}",
        "Data is repartitioned by ingest_year/ingest_month before Parquet write to align with frequent temporal filtering.",
        "",
        "## Resource Allocation",
        f"executors = {cfg.get('resources', {}).get('executors', 'default')}",
        f"cores_per_executor = {cfg.get('resources', {}).get('cores_per_executor', 'default')}",
        f"executor_memory = {cfg.get('resources', {}).get('executor_memory', 'default')}",
    ]
    (reports_dir / "engineering_notes.md").write_text("\n".join(notes), encoding="utf-8")


def run_pipeline(
    base_dir: Path,
    config_path: Path,
    dataset_url: str,
    resource_url: str | None,
    run_ml: bool,
    cv_parallelism: int,
) -> int:
    dirs = ensure_dirs(base_dir)
    lineage_path = dirs["reports"] / "pipeline_lineage.jsonl"
    if lineage_path.exists():
        lineage_path.unlink()

    spark: SparkSession | None = None
    try:
        cfg = load_spark_config(config_path)

        resolved_url = resolve_resource_url(dataset_url=dataset_url, resource_url=resource_url)
        log_lineage(lineage_path, "resolve_resource", "ok", {"resolved_url": resolved_url})

        source = download_resource(resolved_url, dirs["raw"])
        log_lineage(lineage_path, "download", "ok", {"source_path": str(source)})

        spark = create_spark_session(base_dir, config_path)
        raw_df = read_with_spark(spark, source, dirs["raw"])
        valid_df = validate_ingestion(raw_df, dirs["reports"])

        curated_df = apply_processing_pipeline(valid_df, dirs["processed"])  # includes broadcast join + partition write
        log_lineage(lineage_path, "transform", "ok", {"output": str(dirs['processed'] / 'curated')})

        cached = curated_df.persist(StorageLevel.MEMORY_AND_DISK)
        total_rows = cached.count()

        write_csv_single(cached.agg(F.count(F.lit(1)).alias("total_records")), dirs["reports"] / "summary_totals.csv")
        pd.DataFrame([{"total_records": total_rows, "total_columns": len(cached.columns), "source_url": resolved_url}]).to_csv(
            dirs["reports"] / "dataset_profile.csv", index=False
        )

        if "geometry_type" in cached.columns:
            geom = cached.groupBy("geometry_type").count().withColumnRenamed("count", "records").orderBy(F.desc("records"))
            write_csv_single(geom, dirs["reports"] / "geometry_distribution.csv")

        compute_null_profile(cached, dirs["reports"] / "null_profile_top20.csv")
        compute_column_inventory(cached, dirs["reports"] / "column_inventory.csv")
        compute_numeric_statistics(cached, dirs["reports"] / "numeric_statistics.csv")
        compute_top_values(cached, dirs["reports"] / "top_values_by_column.csv")

        cached.limit(500).toPandas().to_csv(
            dirs["reports"] / "sample_records.csv", index=False, quoting=csv.QUOTE_NONNUMERIC
        )

        if run_ml:
            run_mllib(cached, dirs["models"], dirs["reports"], cv_parallelism=cv_parallelism)
            log_lineage(lineage_path, "mllib", "ok", {"output": str(dirs['reports'] / 'mllib_model_comparison.csv')})

        write_executive_summary(dirs["reports"], total_rows=total_rows, total_columns=len(cached.columns))
        write_engineering_notes(dirs["reports"], cfg)

        cached.unpersist()

        log_lineage(lineage_path, "pipeline_complete", "ok", {"rows": total_rows, "run_ml": run_ml})
        print(f"Water bodies pipeline completed. Reports: {dirs['reports']}")
        return 0

    except Exception as exc:  # pylint: disable=broad-except
        log_lineage(lineage_path, "pipeline_complete", "error", {"message": str(exc)})
        print(f"Water bodies pipeline failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run water-bodies Spark pipeline")
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--config", type=Path, default=Path("config/spark_config.yaml"))
    parser.add_argument("--dataset-url", type=str, default=DEFAULT_DATASET_URL)
    parser.add_argument("--resource-url", type=str, default=None)
    parser.add_argument("--skip-ml", action="store_true")
    parser.add_argument("--cv-parallelism", type=int, default=2)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_pipeline(
        base_dir=args.output_dir.resolve(),
        config_path=args.config,
        dataset_url=args.dataset_url,
        resource_url=args.resource_url,
        run_ml=not args.skip_ml,
        cv_parallelism=args.cv_parallelism,
    )


if __name__ == "__main__":
    raise SystemExit(main())
