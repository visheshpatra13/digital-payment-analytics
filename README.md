## Digital Payment Analytics Platform

This project is an end-to-end **Digital Payment Analytics Platform** that demonstrates:

- **Data Warehouse design** (Star schema, with notes for Snowflake schema extensions)
- **Advanced SQL** (CTEs, window functions, indexing, partitioning)
- **Python ETL/ELT pipelines**
- **Apache Spark batch and streaming processing**
- **Snowflake data warehousing features** (clustering, time travel, semi-structured data)
- **Security & Governance** (RBAC, data masking, policies)
- **Performance tuning & optimization**
- **Interactive analytics dashboard** (Streamlit)

### 1. Project structure

- `config/`
  - `config_example.yaml` – example configuration for DBs, Snowflake, and data locations.
- `sql/`
  - `dw_schema.sql` – core data warehouse (star schema) DDL.
  - `advanced_queries.sql` – CTEs, window functions, indexing, and partitioning examples.
  - `snowflake_features.sql` – clustering, time travel, semi-structured, masking, and RBAC examples.
- `etl/`
  - `extract_payments.py` – sample extraction from OLTP-like source into landing zone.
  - `transform_load_dw.py` – transforms landing data into fact/dimension tables (ELT-style).
- `spark/`
  - `batch_job.py` – batch aggregation of payment data using Spark.
  - `streaming_job.py` – structured streaming example for near real-time payment metrics.
- `dashboard/`
  - `app.py` – Streamlit-based analytics dashboard.
- `docs/`
  - `architecture.md` – high-level architecture and data flow.
  - `security_governance.md` – RBAC, masking, and governance patterns.
  - `performance_tuning.md` – tuning guidelines across SQL, Snowflake, and Spark.

### 2. Setup

1. **Create and activate a virtual environment** (recommended):

```bash
python -m venv .venv
.\.venv\Scripts\activate  # Windows
```

2. **Install dependencies**:

```bash
pip install -r requirements.txt
```

3. **Configure environment**:

- Copy `config/config_example.yaml` to `config/config.yaml`.
- Fill in connection details for:
  - Source OLTP / transactional DB (or CSV files).
  - Snowflake account, warehouse, database, schema, role.

### 3. Running the pieces

- **Create DW schema & objects**:
  - Execute `sql/dw_schema.sql` and `sql/snowflake_features.sql` in your Snowflake worksheet or via any SQL client.

- **Run ETL/ELT**:

```bash
python -m etl.extract_payments
python -m etl.transform_load_dw
```

- **Run Spark batch job**:

```bash
spark-submit spark/batch_job.py
```

- **Run Spark streaming job** (requires a source such as Kafka or a file stream):

```bash
spark-submit spark/streaming_job.py
```

- **Run the dashboard**:

```bash
streamlit run dashboard/app.py
```

### 4. Notes

- The code and SQL here are **reference implementations** meant to be adapted to your environment.
- Snowflake-specific features (e.g., clustering keys, masking policies) require appropriate Snowflake editions and privileges.
- For production use, integrate with your CI/CD, secrets management, monitoring, and data catalog.

