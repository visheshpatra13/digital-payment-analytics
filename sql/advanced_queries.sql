USE DATABASE PAYMENTS_DWH;
USE SCHEMA CORE;

-- 1. Common Table Expressions (CTEs) for reusable logic

WITH daily_payments AS (
    SELECT
        DATE_TRUNC('day', AUTHORIZED_AT) AS payment_date,
        MERCHANT_KEY,
        COUNT(*)                         AS txn_count,
        SUM(AMOUNT_CAPTURED)             AS amount_captured
    FROM FACT_PAYMENT
    WHERE STATUS = 'CAPTURED'
    GROUP BY 1, 2
),
merchant_dim AS (
    SELECT
        MERCHANT_KEY,
        MERCHANT_NAME,
        CATEGORY,
        COUNTRY_CODE
    FROM DIM_MERCHANT
)
SELECT
    d.payment_date,
    m.MERCHANT_NAME,
    m.CATEGORY,
    d.txn_count,
    d.amount_captured
FROM daily_payments d
JOIN merchant_dim m
  ON d.MERCHANT_KEY = m.MERCHANT_KEY
ORDER BY d.payment_date DESC, d.amount_captured DESC
LIMIT 100;

-- 2. Window functions: rolling sums, ranking merchants by volume

SELECT
    DATE_TRUNC('day', AUTHORIZED_AT) AS payment_date,
    MERCHANT_KEY,
    SUM(AMOUNT_CAPTURED)                               AS amount_captured,
    SUM(SUM(AMOUNT_CAPTURED)) OVER (
        PARTITION BY MERCHANT_KEY
        ORDER BY DATE_TRUNC('day', AUTHORIZED_AT)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_amount,
    RANK() OVER (
        PARTITION BY DATE_TRUNC('day', AUTHORIZED_AT)
        ORDER BY SUM(AMOUNT_CAPTURED) DESC
    ) AS merchant_rank_by_day
FROM FACT_PAYMENT
WHERE STATUS = 'CAPTURED'
GROUP BY 1, 2
ORDER BY payment_date DESC, merchant_rank_by_day
LIMIT 200;

-- 3. Indexing / micro-partitioning notes (Snowflake)
-- Snowflake uses micro-partitions rather than traditional B-tree indexes.
-- You can influence pruning with appropriate clustering and column usage.

-- Example: optimize queries by clustering FACT_PAYMENT
ALTER TABLE FACT_PAYMENT CLUSTER BY (DATE_KEY, MERCHANT_KEY, STATUS);

-- 4. Partitioning example (for non-Snowflake warehouses)
-- For platforms that support table partitioning (e.g., Postgres, BigQuery),
-- you would partition FACT_PAYMENT by payment_date or date_key. This file keeps
-- Snowflake SQL but documents the concept.

