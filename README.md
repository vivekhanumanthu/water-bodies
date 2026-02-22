# Water Bodies Big Data Platform (PySpark + Tableau)

Dataset:
- Water Bodies: https://catalog.data.gov/dataset/water-bodies-07739

## Setup

```bash
bash scripts/setup_environment.sh
source .venv/bin/activate
```

## Run Full Pipeline (Engineering + ML)

```bash
python scripts/run_data_platform.py --dataset water_bodies --cv-parallelism 2
```

Run engineering only:

```bash
python scripts/run_data_platform.py --dataset water_bodies --skip-ml
```

Run full assessment workflow (pipeline + scalability in sequence):

```bash
python scripts/run_full_assessment.py
```

Run scalability analysis:

```bash
python scripts/water_scalability_profiler.py --partitions 2,4,8 --fixed-rows 100000 --rows-per-partition 30000
```

Compare old vs new Spark implementation (same data/task):

```bash
python scripts/water_pipeline_compare_modes.py --mode both
```

Comparison outputs:
- `reports/water_bodies/comparison/comparison_runtime.csv`
- `reports/water_bodies/comparison/old/plan_transform.txt`
- `reports/water_bodies/comparison/new/plan_transform.txt`
- `reports/water_bodies/comparison/old/*.csv`
- `reports/water_bodies/comparison/new/*.csv`

If Data.gov resource auto-resolution fails, use direct resource URL:

```bash
python scripts/run_data_platform.py --dataset water_bodies --water-resource-url "<direct_csv_or_geojson_url>"
```

## Output Reports

Primary reports in `reports/water_bodies/`:
- `summary_totals.csv`
- `dataset_profile.csv`
- `ingestion_validation.csv`
- `null_profile_top20.csv`
- `column_inventory.csv`
- `numeric_statistics.csv`
- `top_values_by_column.csv`
- `geometry_distribution.csv` (if geometry present)
- `sample_records.csv`
- `executive_summary.md`
- `engineering_notes.md`
- `pipeline_lineage.jsonl`

ML reports in `reports/water_bodies/`:
- `mllib_model_comparison.csv`
- `sklearn_baseline.csv`
- `feature_importance_index.csv` (for tree models)
- `split_class_distribution.csv`

Scalability reports in `reports/water_bodies/`:
- `scaling_strong.csv`
- `scaling_weak.csv`
- `scaling_cost_performance.csv`
- `bottleneck_analysis.json`

Model artifacts in `data/models/water_bodies/`:
- `*_spark_model/`
- `sklearn_baseline.pkl`

## Tableau

- Build instructions: `tableau/WATER_BODIES_4_DASHBOARDS.md`
- General notes: `tableau/README_tableau.md`

## Coverage Mapping

- Full requirement mapping: `docs/REQUIREMENT_COVERAGE.md`
