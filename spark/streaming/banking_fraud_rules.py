
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    unix_timestamp,
    when,
    lit,
    count,
    sum as spark_sum,
    avg,
    collect_set,
    size,
    concat_ws,
    upper
)
from pyspark.sql.window import Window

PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "fraud_detection_deduplicated"

CHECKPOINT_PATH = PROJECT_ROOT / "checkpoints" / "banking-fraud-rules-deduplicated"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
KAFKA_TOPIC = "banking-transactions"

spark = (
    SparkSession.builder
    .appName("BankingAdvancedFraudDetection")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Connecting to Kafka...")

transaction_schema = """
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    transaction_amount DOUBLE,
    transaction_type STRING,
    merchant_category STRING,
    transaction_location STRING,
    transaction_timestamp STRING,
    device_id STRING,
    transaction_status STRING,
    is_fraud BOOLEAN
"""

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .selectExpr(
        f"from_json(json_value, '{transaction_schema}') AS transaction"
    )
    .select("transaction.*")
)

cleaned_df = (
    parsed_df
    .withColumn(
        "transaction_timestamp",
        to_timestamp(col("transaction_timestamp"))
    )
    .withColumn(
        "transaction_status",
        # Normalize status values for consistent rule matching.
        col("transaction_status")
    )
    .filter(
        col("transaction_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("transaction_amount").isNotNull()
        & (col("transaction_amount") > 0)
        & col("transaction_timestamp").isNotNull()
    )
)
# Deduplicate transactions across streaming micro-batches.
# transaction_id is unique in the original transaction dataset.
cleaned_df = cleaned_df.dropDuplicates(["transaction_id"])

def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No transactions received")
        return

    print("\n========================================")
    print(f"FRAUD DETECTION - BATCH {batch_id}")
    print("========================================")

    # Convert transaction time into numeric seconds for range-based windows.
    batch_df = (
        batch_df
        .withColumn(
            "event_time_epoch",
            unix_timestamp(col("transaction_timestamp"))
        )
        .withColumn(
            "transaction_status",
            # Normalize status values before checking failed payments.
            # The uppercase operation is applied below.
            col("transaction_status")
        )
        .withColumn(
            "transaction_status",
            __import__("pyspark.sql.functions", fromlist=["upper"])
            .upper(col("transaction_status"))
        )
        
        
    )

    # Customer activity during the previous 120 seconds.
    two_minute_window = (
        Window
        .partitionBy("customer_id")
        .orderBy(col("event_time_epoch"))
        .rangeBetween(-120, 0)
    )

    # Customer average within this micro-batch.
    customer_window = Window.partitionBy("customer_id")

    enriched_df = (
        batch_df
        .withColumn(
            "transactions_2m",
            count("*").over(two_minute_window)
        )
        .withColumn(
            "locations_2m",
            collect_set("transaction_location").over(two_minute_window)
        )
        .withColumn(
            "location_count_2m",
            size(col("locations_2m"))
        )
        .withColumn(
            "failed_payments_2m",
            spark_sum(
                when(col("transaction_status") == "FAILED", 1)
                .otherwise(0)
            ).over(two_minute_window)
        )
        .withColumn(
            "customer_avg_amount",
            avg("transaction_amount").over(customer_window)
        )
    )

    # Rule 1: High-value transaction.
    enriched_df = enriched_df.withColumn(
        "high_value_flag",
        col("transaction_amount") >= 100000
    )

    # Rule 2: More than five transactions within two minutes.
    enriched_df = enriched_df.withColumn(
        "velocity_flag",
        col("transactions_2m") > 5
    )

    # Rule 3: Transactions from multiple locations within two minutes.
    enriched_df = enriched_df.withColumn(
        "location_anomaly_flag",
        (col("location_count_2m") > 1)
        & (col("transactions_2m") >= 2)
    )

    # Rule 4: Three or more failed payments within two minutes.
    enriched_df = enriched_df.withColumn(
        "repeated_failure_flag",
        col("failed_payments_2m") >= 3
    )

    # Rule 5: Unusual spending compared with this batch's customer average.
    enriched_df = enriched_df.withColumn(
        "unusual_spending_flag",
        (col("transaction_amount") >= 50000)
        & (col("transaction_amount") > col("customer_avg_amount") * 3)
    )

    # Combine all fraud rules.
    enriched_df = enriched_df.withColumn(
        "rule_flagged_fraud",
        col("high_value_flag")
        | col("velocity_flag")
        | col("location_anomaly_flag")
        | col("repeated_failure_flag")
        | col("unusual_spending_flag")
    )

    # Explain why each transaction was flagged.
    enriched_df = enriched_df.withColumn(
        "fraud_reason",
        concat_ws(
            ", ",
            when(col("high_value_flag"), lit("HIGH_VALUE_TRANSACTION")),
            when(col("velocity_flag"), lit("HIGH_TRANSACTION_VELOCITY")),
            when(col("location_anomaly_flag"), lit("MULTIPLE_LOCATIONS")),
            when(col("repeated_failure_flag"), lit("REPEATED_FAILED_PAYMENTS")),
            when(col("unusual_spending_flag"), lit("UNUSUAL_SPENDING"))
        )
    )

    print("\nTransaction fraud results:")

    enriched_df.select(
        "transaction_id",
        "customer_id",
        "transaction_amount",
        "transaction_location",
        "transactions_2m",
        "location_count_2m",
        "failed_payments_2m",
        "high_value_flag",
        "velocity_flag",
        "location_anomaly_flag",
        "repeated_failure_flag",
        "unusual_spending_flag",
        "rule_flagged_fraud",
        "fraud_reason"
    ).show(20, truncate=False)

    print("\nFraud detection summary:")

    enriched_df.agg(
        count("*").alias("total_transactions"),
        spark_sum(
            when(col("rule_flagged_fraud"), 1).otherwise(0)
        ).alias("flagged_transactions"),
        spark_sum(
            when(col("is_fraud"), 1).otherwise(0)
        ).alias("actual_fraud_transactions")
    ).show(truncate=False)

    # Save enriched transactions, including the rule results.
    (
        enriched_df
        .drop("event_time_epoch", "locations_2m", "customer_avg_amount")
        .write
        .mode("append")
        .parquet(OUTPUT_PATH.as_uri())
    )

    print(f"Batch {batch_id} saved to: {OUTPUT_PATH}")


print("\nStarting advanced fraud detection stream...")

query = (
    cleaned_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH.as_uri())
    .trigger(processingTime="10 seconds")
    .start()
)

print("Fraud detection stream is running.")
print("Waiting for banking transactions...")
print("Press CTRL+C to stop.")

try:
    query.awaitTermination()

except KeyboardInterrupt:
    print("\nStopping fraud detection stream...")
    query.stop()

finally:
    spark.stop()
    print("Fraud detection application stopped.")
