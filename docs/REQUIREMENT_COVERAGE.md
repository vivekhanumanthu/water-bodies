# Requirement Coverage Matrix

This file maps the requested assessment criteria to implementation evidence in this repository.

## 1) PySpark Data Engineering

### a) Data Ingestion and Storage Design
- Efficient SparkSession configuration:
  - `scripts/pipeline_common.py` (`create_spark_session`)
  - Config-driven resources/shuffle/UI from `config/spark_config.yaml`
- Partitioning strategy aligned with query patterns:
  - `scripts/run_water_bodies_pipeline.py` (`apply_processing_pipeline`)
  - Parquet partition columns: `ingest_year`, `ingest_month`
- Storage format choice (Parquet):
  - Curated write uses Parquet in `data/processed/water_bodies/curated`
- Data validation at ingestion:
  - `validate_ingestion` writes `reports/water_bodies/ingestion_validation.csv`

### b) Distributed Data Processing Pipeline
- Broadcast join where appropriate:
  - Broadcast join with tiny quality dimension in `apply_processing_pipeline`
- Memory management (`persist`/`unpersist`):
  - Cached reporting stage in `run_pipeline` (`MEMORY_AND_DISK` + `unpersist`)
- Error handling and lineage:
  - Structured try/except with `PipelineError`
  - Step lineage logs in `reports/water_bodies/pipeline_lineage.jsonl`

### c) Performance Optimization
- DataFrame vs RDD usage justification:
  - Documented in `reports/water_bodies/engineering_notes.md`
- Caching strategy documentation:
  - Documented in `engineering_notes.md`
- Spark UI screenshots evidence:
  - Destination folder: `reports/water_bodies/spark_ui/`
- Shuffle management and partition tuning:
  - `spark.sql.shuffle.partitions` in `config/spark_config.yaml`
  - repartitioning before Parquet write in pipeline

## 2) Scalability and Distributed ML

### a) PySpark MLlib Implementation
- At least 3 MLlib algorithms:
  - Logistic Regression, Random Forest, GBT in `run_mllib`
- Compare with scikit-learn baseline:
  - `run_sklearn_baseline` -> `reports/water_bodies/sklearn_baseline.csv`
- Custom transformer implementation:
  - `WaterProfileTransformer` (domain-specific completeness features)
- Model serialization (MLlib/Pickle):
  - Spark model save to `data/models/water_bodies/*_spark_model`
  - sklearn pickle `data/models/water_bodies/sklearn_baseline.pkl`

### b) Distributed Training & Hyperparameter Tuning
- CrossValidator with parallelism:
  - `CrossValidator(... parallelism=cv_parallelism)` in `run_mllib`
- Hyperparameter grid with computational constraints:
  - Algorithm-specific bounded grids in `run_mllib`
- Model checkpointing:
  - `train_df.checkpoint(eager=True)` in `run_mllib`
- Resource allocation justification:
  - `config/spark_config.yaml` and `engineering_notes.md`

### c) Scalability Analysis
- Strong scaling:
  - `scripts/water_scalability_profiler.py` -> `scaling_strong.csv`
- Weak scaling:
  - `scripts/water_scalability_profiler.py` -> `scaling_weak.csv`
- Bottleneck identification:
  - `bottleneck_analysis.json`
- Cost-performance tradeoff:
  - `scaling_cost_performance.csv`

## 3) Tableau Visualization

### a) Big Data Visualization Strategy
- Dashboard 1: Data quality + pipeline monitoring
- Dashboard 2: Model performance + feature importance
- Dashboard 3: Business insights + recommendations
- Dashboard 4: Scalability + cost analysis
- Build spec: `tableau/WATER_BODIES_4_DASHBOARDS.md`

### b) Visualization Best Practices
- Tableau extracts, LOD, parameter controls, mobile design guidance:
  - `tableau/WATER_BODIES_4_DASHBOARDS.md`

### c) Data Storytelling
- Narrative flow, annotations, action filters, export guidance:
  - `tableau/WATER_BODIES_4_DASHBOARDS.md`

## 4) Model Evaluation

### a) Distributed Evaluation Metrics
- Train/validation/test split with temporal considerations:
  - `stratified_temporal_split` uses event order and temporal percentile boundaries
- Cross-validation with stratification for imbalanced data:
  - Stratified split by label before CV (`stratified_temporal_split`)
  - CV run via Spark `CrossValidator`
- Statistical significance (bootstrap CI):
  - `bootstrap_auc_ci` and CI columns in `mllib_model_comparison.csv`
- Business metric alignment:
  - `expected_profit_index` in `mllib_model_comparison.csv`

## Core Execution Commands
- Full pipeline: `python scripts/run_data_platform.py --dataset water_bodies --cv-parallelism 2`
- Scalability: `python scripts/water_scalability_profiler.py --partitions 2,4,8 --fixed-rows 100000 --rows-per-partition 30000`
