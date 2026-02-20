# Traffic Crashes Big Data Project (PySpark + Tableau)

Dataset used:
- Data.gov catalog: https://catalog.data.gov/dataset/traffic-crashes-crashes
- Source API: `https://data.cityofchicago.org/resource/85ca-t3if.json`

## Repository Structure

```text
project/
├── notebooks/
│ ├── 1_data_ingestion.ipynb
│ ├── 2_feature_engineering.ipynb
│ ├── 3_model_training.ipynb
│ └── 4_evaluation.ipynb
├── tableau/
│ ├── dashboard1.twbx
│ ├── dashboard2.twbx
│ ├── dashboard3.twbx
│ ├── dashboard4.twbx
│ └── README_tableau.md
├── scripts/
│ ├── setup_environment.sh
│ ├── run_pipeline.py
│ └── performance_profiler.py
├── config/
│ ├── spark_config.yaml
│ └── tableau_config.json
├── data/
│ ├── schemas/
│ └── samples/
├── tests/
│ └── test_pipeline.py
├── .gitignore
├── environment.yml
├── Dockerfile
├── README.md
└── traffic_crashes_pipeline.py
```

## Setup

### venv
```bash
bash scripts/setup_environment.sh
source .venv/bin/activate
```

### conda
```bash
conda env create -f environment.yml
conda activate traffic-crashes
```

## Core Runs

Data engineering + distributed ML:
```bash
python scripts/run_pipeline.py --max-rows 300000 --cv-parallelism 2
```

Data engineering only:
```bash
python scripts/run_pipeline.py --max-rows 300000 --skip-ml
```

Scalability experiments:
```bash
python scripts/performance_profiler.py --partitions 2,4,8 --fixed-rows 300000 --rows-per-partition 100000
```

## Requirement Mapping

### 1) PySpark Data Engineering
- SparkSession configuration: `config/spark_config.yaml` + `create_spark_session()` in `scripts/run_pipeline.py`
- Partitioning strategy: Parquet partition by `crash_year`, `crash_month` in `create_clean_table()`
- Storage format: Parquet output to `data/processed/crashes_clean/`
- Data validation: `validate_raw_data()` and `reports/data_quality_summary.csv`
- Broadcast join: weather lookup join with `broadcast()` in `create_clean_table()`
- Memory management: `persist(MEMORY_AND_DISK)` / `unpersist()` in `analyze()`
- Error handling + lineage: exception handling + `reports/pipeline_lineage.jsonl`
- Shuffle tuning: configurable `spark.sql.shuffle.partitions`

### 2) Scalability and Distributed ML
- 3 MLlib algorithms: Logistic Regression, Random Forest, GBT in `run_mllib_experiments()`
- scikit-learn baseline: `run_sklearn_baseline()` outputs `reports/sklearn_baseline.csv`
- Custom transformer: `RushHourTransformer`
- Serialization: Spark model save to `data/models/*_spark_model`, sklearn pickle to `data/models/sklearn_logistic.pkl`
- CrossValidator + grid search: implemented per algorithm with configurable parallelism
- Checkpointing: `train_df.checkpoint(eager=True)`
- Resource config: `config/spark_config.yaml` under `resources`
- Strong/Weak scaling + bottlenecks + cost: `scripts/performance_profiler.py` outputs
  - `reports/scaling_strong.csv`
  - `reports/scaling_weak.csv`
  - `reports/scaling_cost_performance.csv`
  - `reports/bottleneck_analysis.json`

### 3) Tableau Visualization
- Dashboard strategy and best practices: `tableau/README_tableau.md`
- Dashboard artifacts: `tableau/dashboard1.twbx` ... `tableau/dashboard4.twbx`
- Config and LOD/parameter guidance: `config/tableau_config.json`

### 4) Model Evaluation
- Temporal split (train/val/test): `temporal_split()`
- Cross-validation: `CrossValidator` in `run_mllib_experiments()`
- Bootstrap confidence intervals: `bootstrap_auc_ci()`
- Business metric alignment: `expected_cost_avoided_index` in `reports/mllib_model_comparison.csv`

## Spark UI Evidence
Store Spark UI screenshots under:
- `reports/spark_ui/`

Include screenshots of:
- physical plan before/after optimization
- stage DAG for joins/shuffles
- executor/storage tabs to justify caching decisions

## Tests

```bash
pytest -q
```

## Docker

```bash
docker build -t traffic-crashes .
docker run --rm -it traffic-crashes
```
