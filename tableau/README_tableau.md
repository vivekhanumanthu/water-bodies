# Tableau Visualization Strategy

Detailed implementation steps (including setup) are in:
- `tableau/BUILD_STEPS.md`

## Dashboard 1: Data Quality and Pipeline Monitoring
- Data quality metrics from `reports/data_quality_summary.csv`
- Pipeline lineage status from `reports/pipeline_lineage.jsonl`
- Ingestion volume trend from `reports/crashes_by_month.csv`

## Dashboard 2: Model Performance and Feature Importance
- Model comparison from `reports/mllib_model_comparison.csv`
- Single-node baseline from `reports/sklearn_baseline.csv`
- Add feature importance for tree-based model (export from Spark notebook)

## Dashboard 3: Business Insights and Recommendations
- Crash causes and monthly trend (`top_20_primary_causes.csv`, `crashes_by_month.csv`)
- Risk segmentation by weather risk band
- Recommendation annotations for interventions

## Dashboard 4: Scalability and Cost Analysis
- Strong scaling (`reports/scaling_strong.csv`)
- Weak scaling (`reports/scaling_weak.csv`)
- Cost-performance (`reports/scaling_cost_performance.csv`)

## Best Practices Checklist
- Use Tableau extracts for large CSVs
- Use LOD expressions for stable aggregation baselines
- Add parameters for year window/model selection
- Validate mobile layout for each dashboard
- Use action filters to connect trend and category views
- Add export buttons (PDF/PNG) for stakeholder sharing

## Storytelling Flow
1. Data reliability and pipeline health
2. Model quality and confidence
3. Business impact and decisions
4. Scalability tradeoffs and operational recommendations
