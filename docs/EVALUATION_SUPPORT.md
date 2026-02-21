# Evaluation Requirement Support (Evidence)

Dataset: Traffic Crashes - Crashes (Socrata `85ca-t3if`)

This document maps each requested evaluation criterion to implementation evidence in the codebase.

## 1) Train/validation/test split with temporal considerations
Status: Implemented

Evidence:
- `scripts/run_pipeline.py:403` defines `temporal_split(...)`.
- `scripts/run_pipeline.py:404` converts crash timestamp to epoch (`event_epoch`).
- `scripts/run_pipeline.py:405` computes temporal cut points using quantiles.
- `scripts/run_pipeline.py:406` to `scripts/run_pipeline.py:408` create train/val/test chronologically.
- `scripts/run_pipeline.py:463` applies temporal split before model training.

Output impact:
- Validation and test metrics in `reports/mllib_model_comparison.csv` come from temporally held-out sets.

## 2) Cross-validation with stratification for imbalanced data
Status: Partially implemented

Implemented evidence:
- `scripts/run_pipeline.py:511` to `scripts/run_pipeline.py:517` uses Spark `CrossValidator` (`numFolds=3`) for hyperparameter tuning.

Gap:
- Explicit stratified fold assignment by label is not implemented in the current code.
- Spark `CrossValidator` here performs fold CV but not custom stratified CV.

## 3) Statistical significance testing (bootstrap confidence intervals)
Status: Implemented

Evidence:
- `scripts/run_pipeline.py:412` defines `bootstrap_auc_ci(...)`.
- `scripts/run_pipeline.py:415` to `scripts/run_pipeline.py:419` performs bootstrap resampling and ROC-AUC recomputation.
- `scripts/run_pipeline.py:536` computes CI bounds for each model.
- `scripts/run_pipeline.py:546` and `scripts/run_pipeline.py:547` export CI bounds (`auc_ci_low`, `auc_ci_high`).

Output artifact:
- `reports/mllib_model_comparison.csv`

## 4) Business metric alignment (e.g., expected profit / CLV)
Status: Partially implemented

Implemented evidence:
- `scripts/run_pipeline.py:538` computes a business-oriented proxy (`expected_cost_avoided`).
- `scripts/run_pipeline.py:549` stores `expected_cost_avoided_index` per model.

Current limitation:
- Metric is an index/proxy, not a calibrated expected-profit/CLV model with explicit business assumptions.

Output artifact:
- `reports/mllib_model_comparison.csv`

## Related Evaluation Outputs
- `reports/mllib_model_comparison.csv`: Spark model metrics and confidence intervals.
- `reports/sklearn_baseline.csv`: single-node baseline for comparison.

## Notes for submission wording
You can state:
- Temporal split: fully implemented.
- Cross-validation: implemented; stratified folding is a current enhancement item.
- Statistical significance: implemented via bootstrap CI.
- Business alignment: implemented as expected-cost-avoided index; can be extended to explicit profit/CLV.
