# Big Data Assignment 1 - Water Bodies Pipeline

## Project Goal
Build an end-to-end Spark data platform for the Water Bodies dataset with:
- ingestion and validation
- distributed processing and curated outputs
- ML model training/evaluation
- scalability and runtime comparison reports
- Tableau-ready reporting artifacts

## Repository Structure
- `scripts/`: pipeline runners and profiling/comparison scripts
- `config/`: Spark + dataset configuration
- `data/`: raw, processed, and model artifacts
- `reports/water_bodies/`: generated CSV/JSON reports
- `tableau/`: Tableau guide and packaged workbooks

## Main Run Commands (from repo root)

```bash
# Full pipeline (engineering + ML)
.venv/bin/python scripts/run_data_platform.py --dataset water_bodies --cv-parallelism 2

# Scalability profiling outputs
.venv/bin/python scripts/water_scalability_profiler.py --partitions 2,4,8 --fixed-rows 100000 --rows-per-partition 30000

# Old vs new processing runtime comparison
.venv/bin/python scripts/water_pipeline_compare_modes.py --mode both --output-dir . --config config/spark_config.yaml
```

If using a direct data resource URL:
- add `--water-resource-url "<direct_url>"` to `run_data_platform.py`
- add `--resource-url "<direct_url>"` to `water_pipeline_compare_modes.py`

## Key Outputs
- `reports/water_bodies/mllib_model_comparison.csv`
- `reports/water_bodies/sklearn_baseline.csv`
- `reports/water_bodies/split_class_distribution.csv`
- `reports/water_bodies/scaling_strong.csv`
- `reports/water_bodies/scaling_weak.csv`
- `reports/water_bodies/scaling_cost_performance.csv`
- `reports/water_bodies/comparison/comparison_runtime.csv`

## Testing
```bash
PYTHONPATH=. .venv/bin/pytest -q
```
