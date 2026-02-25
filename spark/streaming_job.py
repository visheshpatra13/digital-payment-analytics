from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark(app_name: str = "payment_streaming_analytics") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark()

    # Example: file-based stream (for demo). In production, use Kafka or other source.
    input_path = "data/stream/payments"

    schema = (
        "payment_id string, "
        "merchant_id string, "
        "customer_id string, "
        "amount_captured double, "
        "currency string, "
        "status string, "
        "authorized_at timestamp"
    )

    streaming_df = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 1)
        .json(input_path)
    )

    agg = (
        streaming_df.filter(F.col("status") == "CAPTURED")
        .groupBy(
            F.window("authorized_at", "5 minutes"),
            "merchant_id",
            "currency",
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount_captured").alias("amount_captured"),
        )
    )

    query = (
        agg.writeStream.outputMode("update")
        .option("checkpointLocation", "data/checkpoints/payment_stream")
        .format("console")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

