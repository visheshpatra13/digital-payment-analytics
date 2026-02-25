from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark(app_name: str = "payment_batch_aggregations") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark()

    # In a real setup, you would read from Snowflake using the Snowflake connector.
    # Here we assume Parquet extracted from FACT_PAYMENT as an example.
    input_path = "data/warehouse/fact_payment"

    df = spark.read.parquet(input_path)

    daily_metrics = (
        df.filter(F.col("STATUS") == F.lit("CAPTURED"))
        .groupBy(
            F.to_date("AUTHORIZED_AT").alias("payment_date"),
            "MERCHANT_KEY",
            "CURRENCY",
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("AMOUNT_CAPTURED").alias("amount_captured"),
            F.sum("FEES_AMOUNT").alias("fees_amount"),
        )
    )

    # Performance tuning hints:
    # - Repartition by date or merchant if downstream queries filter by them.
    # - Cache intermediate results in iterative workloads.

    output_path = "data/aggregates/daily_merchant_metrics"
    (
        daily_metrics.repartition("payment_date")
        .write.mode("overwrite")
        .partitionBy("payment_date")
        .parquet(output_path)
    )

    print(f"Wrote batch aggregates to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()

