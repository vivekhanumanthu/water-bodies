# Tableau Build Steps (with Setup)

## 1) Setup Tableau Environment

1. Install Tableau Desktop (or use Tableau Public Desktop).
2. Open Tableau and create a new workbook.
3. Set repository/project folder for this assignment.
4. Keep this project folder open in Finder:
   - `reports/`
   - `reports/comparison/`

## 2) Connect Data Sources

Connect these CSV files as separate data sources:

1. `reports/data_quality_summary.csv`
2. `reports/crashes_by_month.csv`
3. `reports/top_20_primary_causes.csv`
4. `reports/mllib_model_comparison.csv`
5. `reports/sklearn_baseline.csv`
6. `reports/scaling_cost_performance.csv`
7. `reports/comparison/comparison_runtime.csv`
8. `reports/comparison/naive/crashes_by_month.csv`
9. `reports/comparison/optimized/crashes_by_month.csv`
10. `reports/comparison/naive/top_20_primary_causes.csv`
11. `reports/comparison/optimized/top_20_primary_causes.csv`

## 3) Data Modeling Tips

1. For each source, verify field data types:
   - dates as Date/DateTime
   - metrics as Number (decimal)
2. For comparison sources, keep `mode` as String/Dimension.
3. Create extract for each source:
   - Data Source page -> Extract -> Save.

## 4) Dashboard 1: Data Quality & Pipeline Monitoring

Create sheets:

1. `DQ Summary`
   - Marks: Text
   - Fields: `total_rows`, `invalid_rows`, `invalid_ratio`
2. `Monthly Trend`
   - Source: `reports/crashes_by_month.csv`
   - Columns: `crash_ym`
   - Rows: `crashes`
   - Marks: Line
3. `Pipeline Status`
   - Optional text panel from latest lineage notes in report

Assemble Dashboard 1:

1. Add `DQ Summary`, `Monthly Trend`, and `Pipeline Status`.
2. Add annotations for data validity and ingestion status.
3. Save as `tableau/dashboard1.twbx`.

## 5) Dashboard 2: Model Performance & Feature Importance

Create sheets:

1. `MLlib Metrics`
   - Source: `reports/mllib_model_comparison.csv`
   - Columns: `model`
   - Rows: `test_roc_auc` (bar)
   - Color: `model`
2. `PR AUC`
   - Columns: `model`
   - Rows: `test_pr_auc`
3. `Spark vs Sklearn`
   - Compare best Spark ROC AUC vs sklearn baseline ROC AUC

Assemble Dashboard 2:

1. Add all metric sheets.
2. Show CI fields (`auc_ci_low`, `auc_ci_high`) in tooltip.
3. Save as `tableau/dashboard2.twbx`.

## 6) Dashboard 3: Business Insights & Recommendations

Create sheets:

1. `Top Causes`
   - Source: `reports/top_20_primary_causes.csv`
   - Columns: `cause`
   - Rows: `crashes`
   - Sort descending
2. `Monthly Volume`
   - Source: `reports/crashes_by_month.csv`
   - Line chart by month
3. `Recommendations`
   - Text sheet with actions based on high-frequency causes

Assemble Dashboard 3:

1. Add `Top Causes` + `Monthly Volume` + `Recommendations` text block.
2. Add action filter from cause chart -> trend chart.
3. Save as `tableau/dashboard3.twbx`.

## 7) Dashboard 4: Scalability + Naive vs Optimized

Create sheets:

1. `Runtime by Mode`
   - Source: `reports/comparison/comparison_runtime.csv`
   - Columns: `mode`
   - Rows: `transform_seconds` (bar)
2. `Query Time Breakdown`
   - Source: `comparison_runtime.csv`
   - Use Measure Names/Values for:
     - `summary_seconds`
     - `monthly_seconds`
     - `top_causes_seconds`
3. `Cost Performance`
   - Source: `reports/scaling_cost_performance.csv`
   - Columns: `partitions`
   - Rows: `estimated_cost`
   - Secondary axis: `rows_per_second`

Assemble Dashboard 4:

1. Add all three sheets.
2. Add annotation for optimized speedup vs naive.
3. Save as `tableau/dashboard4.twbx`.

## 8) Parameter + LOD Setup

Create parameter:

1. `mode_selector` values: `naive`, `optimized`, `both`
2. Use calc field to filter comparison views by parameter.

Create LOD example field:

1. `Fixed Monthly Crashes`
   - Formula: `{ FIXED [crash_ym] : SUM([crashes]) }`

## 9) Mobile-Responsive Setup

1. Open each dashboard -> Device Preview.
2. Add Phone layout.
3. Keep only key KPIs and one chart per phone view.

## 10) Export + Submission Assets

1. Export dashboard images (PNG) for report.
2. Export PDF summary for stakeholders.
3. Keep final packaged files:
   - `tableau/dashboard1.twbx`
   - `tableau/dashboard2.twbx`
   - `tableau/dashboard3.twbx`
   - `tableau/dashboard4.twbx`

## 11) Suggested Build Order

1. Dashboard 4 (comparison evidence first)
2. Dashboard 2 (model metrics)
3. Dashboard 1 (pipeline quality)
4. Dashboard 3 (business story)
