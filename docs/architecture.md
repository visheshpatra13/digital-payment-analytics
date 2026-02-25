## Architecture Overview

- **Source**: OLTP-style `payments`, `customers`, `merchants`, `payment_methods` tables (e.g., Postgres).
- **Landing zone**: Parquet extracts in `data/landing/payments`.
- **Warehouse**: Snowflake database `PAYMENTS_DWH`, schema `CORE`, with a star schema.
- **Processing**:
  - Python ETL/ELT (`etl/`) for extraction and loading to Snowflake staging and dimensions.
  - Spark batch (`spark/batch_job.py`) to compute daily aggregates.
  - Spark streaming (`spark/streaming_job.py`) for near real-time analytics.
- **Consumption**: Streamlit dashboard (`dashboard/app.py`) and ad-hoc SQL.

