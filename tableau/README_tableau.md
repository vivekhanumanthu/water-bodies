# Tableau Creation Guide (Water Bodies)

## 1) Regenerate all required outputs
Run from repo root:

```bash
.venv/bin/python scripts/run_data_platform.py --dataset water_bodies --cv-parallelism 2
.venv/bin/python scripts/water_scalability_profiler.py --partitions 2,4,8 --fixed-rows 100000 --rows-per-partition 30000
.venv/bin/python scripts/water_pipeline_compare_modes.py --mode both --output-dir . --config config/spark_config.yaml
```

## 2) Connect these files in Tableau
- `reports/water_bodies/summary_totals.csv`
- `reports/water_bodies/dataset_profile.csv`
- `reports/water_bodies/ingestion_validation.csv`
- `reports/water_bodies/null_profile_top20.csv`
- `reports/water_bodies/column_inventory.csv`
- `reports/water_bodies/numeric_statistics.csv`
- `reports/water_bodies/top_values_by_column.csv`
- `reports/water_bodies/mllib_model_comparison.csv`
- `reports/water_bodies/sklearn_baseline.csv`
- `reports/water_bodies/split_class_distribution.csv`
- `reports/water_bodies/scaling_strong.csv`
- `reports/water_bodies/scaling_weak.csv`
- `reports/water_bodies/scaling_cost_performance.csv`
- `reports/water_bodies/comparison/comparison_runtime.csv`
- `reports/water_bodies/comparison/old/quality_band_distribution.csv`
- `reports/water_bodies/comparison/new/quality_band_distribution.csv`

Optional (if present):
- `reports/water_bodies/geometry_distribution.csv`
- `reports/water_bodies/feature_importance_index.csv`

## 3) Build 4 dashboards
1. Data Quality and Pipeline Monitoring
2. Model Performance and Feature Importance
3. Business Insights
4. Scalability and Runtime Comparison

## 4) ROC chart fix (important)
- Use `AVG([test_roc_auc])` and `AVG([roc_auc_test_sample])`, not `SUM`.
- Filter out `Model = Null`.
- Avoid accidental many-to-many joins between Spark and sklearn tables.
- If needed, keep Spark and sklearn ROC in separate sheets, then place both in one dashboard.

## 5) Save outputs
- `tableau/dashboard1.twbx`
- `tableau/dashboard2.twbx`
- `tableau/dashboard3.twbx`
- `tableau/dashboard4.twbx`
