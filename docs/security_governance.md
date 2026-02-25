## Security, RBAC, and Governance

- **RBAC**:
  - Roles like `PAYMENTS_READONLY` and `PAYMENTS_ANALYST`.
  - Warehouse and schema usage grants only to appropriate roles.
  - Object privileges (SELECT/INSERT/UPDATE/DELETE) aligned to duties.

- **Data masking**:
  - Dynamic masking policy for email addresses (see `sql/snowflake_features.sql`).
  - Extend similarly for phone, card tokens, and other PII.

- **Row-level security**:
  - Row access policy restricting which countries a role can see.

- **Governance**:
  - Use Snowflake time travel and cloning for safe experimentation and recovery.
  - Track lineage from OLTP to DW via ETL code and SQL scripts.

