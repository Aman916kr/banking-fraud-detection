
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_date,
    when,
    lit,
    count,
    sum as spark_sum
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType
)


# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "streaming"
)

CHECKPOINT_PATH = (
    PROJECT_ROOT / "checkpoints" / "banking-streaming"
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"

KAFKA_TOPIC = "banking-transactions"


# --------------------------------------------------
# 2. DEFINE TRANSACTION JSON SCHEMA
# --------------------------------------------------

transaction_schema = StructType([

    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_id", StringType(), True),

    StructField("transaction_amount", DoubleType(), True),

    StructField("transaction_type", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("transaction_location", StringType(), True),

    StructField("transaction_timestamp", StringType(), True),

    StructField("device_id", StringType(), True),
    StructField("transaction_status", StringType(), True),

    StructField("is_fraud", BooleanType(), True)

])


# --------------------------------------------------
# 3. CREATE SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("BankingFraudStructuredStreaming")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 4. READ STREAM FROM KAFKA
# --------------------------------------------------

print("\nConnecting to Kafka...")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


# --------------------------------------------------
# 5. CONVERT KAFKA JSON INTO STRUCTURED DATA
# --------------------------------------------------

parsed_df = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            transaction_schema
        ).alias("transaction")
    )
    .select("transaction.*")
)


# --------------------------------------------------
# 6. DATA CLEANING
# --------------------------------------------------

cleaned_df = (
    parsed_df
    .filter(
        col("transaction_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("transaction_amount").isNotNull()
        & (col("transaction_amount") > 0)
        & col("transaction_timestamp").isNotNull()
    )
    .withColumn(
        "transaction_timestamp",
        to_timestamp(col("transaction_timestamp"))
    )
    .filter(col("transaction_timestamp").isNotNull())
    .withColumn(
        "transaction_date",
        to_date(col("transaction_timestamp"))
    )
)


# --------------------------------------------------
# 7. RULE-BASED FRAUD DETECTION
# --------------------------------------------------

fraud_df = (
    cleaned_df
    .withColumn(
        "rule_flagged_fraud",
        when(
            col("transaction_amount") >= 100000,
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "fraud_reason",
        when(
            col("transaction_amount") >= 100000,
            lit("HIGH_VALUE_TRANSACTION")
        ).otherwise(lit("NORMAL"))
    )
)


# --------------------------------------------------
# 8. PROCESS EACH MICRO-BATCH
# --------------------------------------------------

def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: No transactions received")
        return

    print("\n===================================")
    print(f"PROCESSING BATCH: {batch_id}")
    print("===================================")

    total = batch_df.count()

    flagged = (
        batch_df
        .filter(col("rule_flagged_fraud") == True)
        .count()
    )

    print("Total transactions:", total)
    print("Rule-flagged transactions:", flagged)

    print("\nTransaction details:")

    batch_df.select(
        "transaction_id",
        "customer_id",
        "transaction_amount",
        "transaction_location",
        "is_fraud",
        "rule_flagged_fraud",
        "fraud_reason"
    ).show(20, truncate=False)

    print("\nFraud statistics:")

    batch_df.agg(
        count("*").alias("total_transactions"),

        spark_sum(
            when(
                col("rule_flagged_fraud"),
                1
            ).otherwise(0)
        ).alias("rule_flagged_transactions"),

        spark_sum(
            when(
                col("is_fraud"),
                1
            ).otherwise(0)
        ).alias("actual_fraud_transactions")

    ).show(truncate=False)

    # Save processed records
    (
        batch_df.write
        .mode("append")
        .parquet(OUTPUT_PATH.as_uri())
    )

    print(f"Batch {batch_id} saved successfully.")


# --------------------------------------------------
# 9. START STREAMING QUERY
# --------------------------------------------------

print("\nStarting Spark Structured Streaming...")

query = (
    fraud_df.writeStream
    .foreachBatch(process_batch)
.option("checkpointLocation", CHECKPOINT_PATH.as_uri())    .trigger(processingTime="10 seconds")
    .start()
)

print("\nStreaming application is running.")
print("Waiting for Kafka transactions...")
print("Press CTRL+C to stop.")

try:
    query.awaitTermination()

except KeyboardInterrupt:
    print("\nStopping streaming application...")

    query.stop()

finally:
    spark.stop()

    print("Spark streaming application stopped.")
