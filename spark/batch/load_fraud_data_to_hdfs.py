
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Existing Step 7 Parquet output on the local Windows drive,
# accessed through WSL.
LOCAL_INPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "fraud_detection"
).as_uri()

# HDFS destination.
HDFS_BASE_PATH = "hdfs://localhost:9000/banking-fraud-detection"

ALL_PATH = f"{HDFS_BASE_PATH}/processed/all_transactions"
NORMAL_PATH = f"{HDFS_BASE_PATH}/processed/normal_transactions"
FRAUD_PATH = f"{HDFS_BASE_PATH}/processed/fraud_transactions"
ALERT_PATH = f"{HDFS_BASE_PATH}/alerts/fraud_alerts"

spark = (
    SparkSession.builder
    .appName("BankingFraudHDFSDataLake")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n======================================")
print("BANKING FRAUD DETECTION - HDFS LOADER")
print("======================================")

print("\nReading Step 7 Parquet data...")
print("Source:", LOCAL_INPUT_PATH)

df = spark.read.parquet(LOCAL_INPUT_PATH)

# Add a date column for partitioned data lake storage.
df = df.withColumn(
    "transaction_date",
    to_date(col("transaction_timestamp"))
)

# Separate normal and fraud transactions.
fraud_df = df.filter(col("rule_flagged_fraud") == True)

normal_df = df.filter(col("rule_flagged_fraud") == False)

# Create a focused fraud alert dataset.
alert_df = fraud_df.select(
    "transaction_id",
    "customer_id",
    "account_id",
    "transaction_amount",
    "transaction_type",
    "merchant_category",
    "transaction_location",
    "transaction_timestamp",
    "transaction_date",
    "transaction_status",
    "fraud_reason",
    "rule_flagged_fraud",
    "is_fraud"
)

# Calculate counts before writing.
total_count = df.count()
fraud_count = fraud_df.count()
normal_count = normal_df.count()
alert_count = alert_df.count()

print("\nSource data summary")
print("-------------------")
print("Total transactions:", total_count)
print("Fraud transactions:", fraud_count)
print("Normal transactions:", normal_count)
print("Fraud alerts:", alert_count)

# Initial HDFS load.
# OVERWRITE makes this initial load repeatable.
# It replaces only these four destination datasets.
print("\nWriting all transactions to HDFS...")

(
    df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("transaction_date")
    .parquet(ALL_PATH)
)

print("Writing normal transactions to HDFS...")

(
    normal_df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("transaction_date")
    .parquet(NORMAL_PATH)
)

print("Writing fraud transactions to HDFS...")

(
    fraud_df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("transaction_date")
    .parquet(FRAUD_PATH)
)

print("Writing fraud alerts to HDFS...")

(
    alert_df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("transaction_date")
    .parquet(ALERT_PATH)
)

print("\n======================================")
print("HDFS DATA LAKE LOAD COMPLETED")
print("======================================")

print("All transactions:", ALL_PATH)
print("Normal transactions:", NORMAL_PATH)
print("Fraud transactions:", FRAUD_PATH)
print("Fraud alerts:", ALERT_PATH)

print("\nExpected record counts:")
print("All:", total_count)
print("Normal:", normal_count)
print("Fraud:", fraud_count)
print("Alerts:", alert_count)

spark.stop()
