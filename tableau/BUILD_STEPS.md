# Tableau Build Steps (Water Bodies)

## 1) Setup

1. Install Tableau Desktop or Tableau Public.
2. Open a new workbook.
3. Keep the project folder open.

## 2) Connect Data Sources

Connect these files:
1. `reports/water_bodies/summary_totals.csv`
2. `reports/water_bodies/dataset_profile.csv`
3. `reports/water_bodies/null_profile_top20.csv`
4. `reports/water_bodies/geometry_distribution.csv` (if present)
5. `reports/water_bodies/sample_records.csv`

Optional lineage timeline:
- Convert `reports/water_bodies/pipeline_lineage.jsonl` to CSV first, then connect.

## 3) Dashboard 1: Data Quality & Pipeline Monitoring

Sheets:
1. `Profile KPIs`: show total records and columns.
2. `Null Top 20`: bar chart from `null_profile_top20.csv`.
3. `Pipeline Timeline`: status/events from lineage CSV (optional).

## 4) Dashboard 2: Water Body Distribution

Sheets:
1. `Geometry Distribution`: bar/pie of `geometry_type` vs `records`.
2. `Total Records`: KPI tile from `summary_totals.csv`.

## 5) Dashboard 3: Completeness & Schema Insights

Sheets:
1. `Null Ratio by Column`: sorted bar chart.
2. `Sample Records Table`: from `sample_records.csv`.

## 6) Dashboard 4: Operational Monitoring

Sheets:
1. `Run Volume Snapshot`: total records over runs (if multiple run outputs kept).
2. `Data Freshness`: latest ingestion timestamp from lineage.

## 7) Best Practices

1. Use Tableau extracts for faster workbook performance.
2. Add parameter controls for column/threshold exploration.
3. Add mobile layouts for each dashboard.
4. Add annotations for key observations.

## 8) Save Outputs

Save packaged dashboards as:
- `tableau/dashboard1.twbx`
- `tableau/dashboard2.twbx`
- `tableau/dashboard3.twbx`
- `tableau/dashboard4.twbx`
