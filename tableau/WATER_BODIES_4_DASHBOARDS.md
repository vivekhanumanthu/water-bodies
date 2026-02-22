# Tableau Guide: 4 Dashboards for Water Bodies (Full Assessment)

Use outputs from:
```bash
python scripts/run_data_platform.py --dataset water_bodies --cv-parallelism 2
python scripts/water_scalability_profiler.py --partitions 2,4,8 --fixed-rows 100000 --rows-per-partition 30000
python scripts/water_pipeline_compare_modes.py --mode both
```

## Connect These Data Sources
- `reports/water_bodies/summary_totals.csv`
- `reports/water_bodies/dataset_profile.csv`
- `reports/water_bodies/ingestion_validation.csv`
- `reports/water_bodies/null_profile_top20.csv`
- `reports/water_bodies/column_inventory.csv`
- `reports/water_bodies/numeric_statistics.csv`
- `reports/water_bodies/top_values_by_column.csv`
- `reports/water_bodies/geometry_distribution.csv` (if available)
- `reports/water_bodies/mllib_model_comparison.csv`
- `reports/water_bodies/sklearn_baseline.csv`
- `reports/water_bodies/split_class_distribution.csv`
- `reports/water_bodies/feature_importance_index.csv` (if available)
- `reports/water_bodies/scaling_strong.csv`
- `reports/water_bodies/scaling_weak.csv`
- `reports/water_bodies/scaling_cost_performance.csv`
- `reports/water_bodies/comparison/comparison_runtime.csv`
- `reports/water_bodies/comparison/old/quality_band_distribution.csv`
- `reports/water_bodies/comparison/new/quality_band_distribution.csv`

## Dashboard 1: Data Quality and Pipeline Monitoring
Sheets:
1. KPI tiles: `total_records`, `total_columns`, `completely_empty_rows`
2. Null ratios by column (`null_profile_top20.csv`)
3. Fill rates by column (`column_inventory.csv`)
4. Optional lineage timeline from `pipeline_lineage.jsonl` converted to CSV

## Dashboard 2: Model Performance and Feature Importance
Sheets:
1. Model ROC-AUC comparison (`mllib_model_comparison.csv`)
2. PR-AUC comparison (`mllib_model_comparison.csv`)
3. Spark vs sklearn baseline (`mllib_model_comparison.csv` + `sklearn_baseline.csv`)
4. Split balance chart (`split_class_distribution.csv`) for stratification evidence
5. Feature importance index (`feature_importance_index.csv`)

## Dashboard 3: Business Insights and Recommendations
Sheets:
1. Top categorical values by selected column (`top_values_by_column.csv`)
2. Numeric medians/dispersion (`numeric_statistics.csv`)
3. Data quality risk bands from null ratio thresholds
4. Text box for recommendations from `executive_summary.md`

## Dashboard 4: Scalability and Cost Analysis
Sheets:
1. Strong scaling curve (`scaling_strong.csv`)
2. Weak scaling curve (`scaling_weak.csv`)
3. Cost vs throughput (`scaling_cost_performance.csv`)
4. Old vs new runtime comparison (`comparison_runtime.csv`)
5. Bottleneck notes from `bottleneck_analysis.json`

## Best Practices Checklist
- Use Tableau Extracts for performance.
- Add LOD field example:
  - `{ FIXED [column] : AVG([null_ratio]) }`
- Add parameter controls:
  - `metric_selector` (`roc_auc`, `pr_auc`, `expected_profit_index`)
  - `column_selector` (for top-values chart)
- Build phone layout for each dashboard.
- Add annotations for key insights and action filters across linked sheets.
- Enable PDF/PNG export for stakeholder sharing.
