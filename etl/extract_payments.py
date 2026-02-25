import os
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
import yaml


def load_config(path: str = "config/config.yaml") -> dict:
    cfg_path = Path(path)
    if not cfg_path.exists():
        cfg_path = Path("config/config_example.yaml")
    with cfg_path.open() as f:
        return yaml.safe_load(f)


def get_payments_engine(cfg: dict) -> sa.Engine:
    src = cfg["sources"]["payments_db"]
    if src["type"] != "postgres":
        raise ValueError("Only postgres is wired in this example.")
    url = sa.URL.create(
        "postgresql+psycopg2",
        username=src["user"],
        password=src["password"],
        host=src["host"],
        port=src["port"],
        database=src["database"],
    )
    return sa.create_engine(url)


def extract_payments(cfg: dict) -> pd.DataFrame:
    engine = get_payments_engine(cfg)
    query = """
        SELECT
            p.payment_id,
            p.customer_id,
            p.merchant_id,
            p.payment_method_id,
            p.authorized_at,
            p.captured_at,
            p.currency,
            p.amount_authorized,
            p.amount_captured,
            p.fees_amount,
            p.status,
            p.failure_reason,
            p.is_refund,
            p.refund_of_payment_id,
            c.full_name,
            c.email,
            c.phone,
            c.country_code    AS customer_country_code,
            c.segment         AS customer_segment,
            m.merchant_name,
            m.category        AS merchant_category,
            m.country_code    AS merchant_country_code,
            pm.method_name,
            pm.scheme,
            pm.is_tokenized
        FROM payments p
        JOIN customers c ON p.customer_id = c.customer_id
        JOIN merchants m ON p.merchant_id = m.merchant_id
        JOIN payment_methods pm ON p.payment_method_id = pm.payment_method_id
        WHERE p.authorized_at >= NOW() - INTERVAL '7 days'
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


def write_to_landing(df: pd.DataFrame, cfg: dict) -> str:
    landing_path = Path(cfg["landing_zone"]["path"])
    landing_path.mkdir(parents=True, exist_ok=True)
    file_path = landing_path / "payments_extract.parquet"
    df.to_parquet(file_path, index=False)
    return str(file_path)


def main() -> None:
    cfg = load_config()
    df = extract_payments(cfg)
    output_path = write_to_landing(df, cfg)
    print(f"Wrote extracted payments to {output_path}")


if __name__ == "__main__":
    main()

