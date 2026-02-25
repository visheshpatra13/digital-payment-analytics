## Performance Tuning and Optimization

- **SQL/Snowflake**:
  - Use clustering keys on large fact tables (`FACT_PAYMENT`) to improve pruning.
  - Avoid SELECT * in heavy queries; project only needed columns.
  - Leverage result caching and warehouse sizing based on workload.
  - Use window functions and CTEs judiciously; materialize where beneficial.

- **Spark**:
  - Tune `spark.sql.shuffle.partitions` based on cluster size and data volume.
  - Repartition data on frequently filtered/grouped keys (e.g., date, merchant).
  - Cache intermediate DataFrames only when reused multiple times.
  - Prefer columnar formats (Parquet) with compression.

- **ETL**:
  - Push down filters and joins into the database where possible.
  - Use bulk loads into Snowflake (staging tables + COPY or MERGE).
  - Monitor pipeline runtimes and add observability (logging/metrics).

