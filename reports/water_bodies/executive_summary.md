# Water Bodies Data Profiling Executive Summary

- Total records processed: **15,103**
- Total columns profiled: **12**

## Key Data Quality Findings
Top columns with highest missingness:
- `DWQREVIEW`: null ratio 99.99% (15,101 nulls)
- `NAME`: null ratio 55.48% (8,379 nulls)
- `HYDRO_TYPE`: null ratio 15.77% (2,382 nulls)
- `quality_band`: null ratio 0.00% (0 nulls)
- `OBJECTID`: null ratio 0.00% (0 nulls)

Most complete columns:
- `OBJECTID`: fill rate 100.00%, distinct values 14,991
- `NAME`: fill rate 100.00%, distinct values 106
- `name_length`: fill rate 100.00%, distinct values 16
- `HYDRO_TYPE`: fill rate 100.00%, distinct values 10
- `non_empty_fields`: fill rate 100.00%, distinct values 4

## Model Evaluation Summary
- Best model: `logistic_regression` with test ROC-AUC `1.0000`
- 95% bootstrap CI: [1.0000, 1.0000]
- Expected profit index: `884400.00`

## Engineering and Optimization Notes
- DataFrame APIs are used throughout for Catalyst optimization and Tungsten execution.
- Broadcast join applied for tiny quality dimension.
- MEMORY_AND_DISK caching used in report stage; explicitly unpersisted after use.
- Partitioning strategy: Parquet partition by ingest_year/ingest_month.
- Spark UI screenshots should be saved under reports/water_bodies/spark_ui/.