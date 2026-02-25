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


def get_snowflake_engine(cfg: dict) -> sa.Engine:
    sf = cfg["snowflake"]
    url = sa.URL.create(
        "snowflake",
        user=sf["user"],
        password=sf["password"],
        host=sf["account"],
        database=sf["database"],
        schema=sf["schema"],
    )
    engine = sa.create_engine(
        url,
        connect_args={
            "warehouse": sf["warehouse"],
            "role": sf["role"],
        },
    )
    return engine


def read_landing(cfg: dict) -> pd.DataFrame:
    landing_path = Path(cfg["landing_zone"]["path"]) / "payments_extract.parquet"
    return pd.read_parquet(landing_path)


def upsert_dimensions(df: pd.DataFrame, engine: sa.Engine) -> None:
    # This is a simplified pattern; real SCD2 logic would use MERGE.
    # For demo, we append new records; deduping should be done in SQL MERGE.
    dim_customer_cols = [
        "customer_id",
        "full_name",
        "email",
        "phone",
        "customer_country_code",
        "customer_segment",
    ]
    dim_customer = (
        df[dim_customer_cols]
        .drop_duplicates(subset=["customer_id"])
        .rename(
            columns={
                "customer_country_code": "country_code",
                "customer_segment": "segment",
            }
        )
    )

    dim_merchant_cols = [
        "merchant_id",
        "merchant_name",
        "merchant_category",
        "merchant_country_code",
    ]
    dim_merchant = (
        df[dim_merchant_cols]
        .drop_duplicates(subset=["merchant_id"])
        .rename(
            columns={
                "merchant_category": "category",
                "merchant_country_code": "country_code",
            }
        )
    )

    dim_payment_method_cols = [
        "payment_method_id",
        "method_name",
        "scheme",
        "is_tokenized",
    ]
    dim_payment_method = df[dim_payment_method_cols].drop_duplicates(
        subset=["payment_method_id"]
    )

    with engine.begin() as conn:
        dim_customer.to_sql(
            "DIM_CUSTOMER_TEMP",
            conn,
            if_exists="replace",
            index=False,
        )
        dim_merchant.to_sql(
            "DIM_MERCHANT_TEMP",
            conn,
            if_exists="replace",
            index=False,
        )
        dim_payment_method.to_sql(
            "DIM_PAYMENT_METHOD_TEMP",
            conn,
            if_exists="replace",
            index=False,
        )


def load_fact(df: pd.DataFrame, engine: sa.Engine) -> None:
    # For demo, we send fact data to a staging table and then use SQL (MERGE) in Snowflake.
    fact_cols = [
        "payment_id",
        "customer_id",
        "merchant_id",
        "payment_method_id",
        "authorized_at",
        "captured_at",
        "currency",
        "amount_authorized",
        "amount_captured",
        "fees_amount",
        "status",
        "failure_reason",
        "is_refund",
        "refund_of_payment_id",
    ]
    fact_df = df[fact_cols].copy()

    with engine.begin() as conn:
        fact_df.to_sql(
            "FACT_PAYMENT_STAGE",
            conn,
            if_exists="replace",
            index=False,
        )


def main() -> None:
    cfg = load_config()
    engine = get_snowflake_engine(cfg)
    df = read_landing(cfg)
    upsert_dimensions(df, engine)
    load_fact(df, engine)
    print("Loaded dimensions (temp) and fact staging into Snowflake.")


if __name__ == "__main__":
    main()

