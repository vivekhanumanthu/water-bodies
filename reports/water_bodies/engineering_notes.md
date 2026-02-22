# Engineering Notes

## DataFrame vs RDD Justification
DataFrame API is used to enable Catalyst optimization, predicate pushdown, and optimized physical planning.
RDD is avoided for core ETL/ML tasks because it loses schema-level optimization benefits.

## Caching Strategy
Curated dataframe is persisted with MEMORY_AND_DISK during repeated report queries and unpersisted immediately after exports.

## Shuffle and Partition Tuning
spark.sql.shuffle.partitions = 120
Data is repartitioned by ingest_year/ingest_month before Parquet write to align with frequent temporal filtering.

## Resource Allocation
executors = 2
cores_per_executor = 2
executor_memory = 4g