
from pathlib import Path
from datetime import datetime, timezone

import happybase
from pyspark.sql import SparkSession


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

HDFS_ALERTS_PATH = (
    "hdfs://localhost:9000/"
    "banking-fraud-detection/alerts/fraud_alerts"
)

HBASE_HOST = "localhost"
HBASE_PORT = 9090

HBASE_TABLE = "banking_fraud:fraud_alerts"


# Fields stored in the transaction column family
TRANSACTION_FIELDS = [
    "transaction_id",
    "customer_id",
    "account_id",
    "transaction_amount",
    "transaction_type",
    "merchant_category",
    "transaction_location",
    "transaction_timestamp",
    "device_id",
    "transaction_status",
]

# Fields stored in the fraud column family
FRAUD_FIELDS = [
    "is_fraud",
    "transactions_2m",
    "location_count_2m",
    "failed_payments_2m",
    "high_value_flag",
    "velocity_flag",
    "location_anomaly_flag",
    "repeated_failure_flag",
    "unusual_spending_flag",
    "rule_flagged_fraud",
    "fraud_reason",
    "transaction_date",
]


def convert_value(value):
    """Convert Spark values into HBase-compatible bytes."""

    if value is None:
        return None

    return str(value).encode("utf-8")


def main():

    print("=" * 60)
    print("BANKING FRAUD ALERTS - HDFS TO HBASE")
    print("=" * 60)

    # --------------------------------------------------
    # 1. READ FRAUD ALERTS FROM HDFS
    # --------------------------------------------------

    spark = (
        SparkSession.builder
        .appName("BankingFraudAlertsToHBase")
        .config(
            "spark.hadoop.fs.defaultFS",
            "hdfs://localhost:9000"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    connection = None

    try:

        print("\nReading fraud alerts from HDFS...")

        df = spark.read.parquet(HDFS_ALERTS_PATH)

        if "transaction_id" not in df.columns:
            raise ValueError(
                "transaction_id column is missing from HDFS data"
            )

        # Remove duplicate transaction IDs
        df = df.dropDuplicates(["transaction_id"])

        total_alerts = df.count()

        print(f"Unique fraud alerts found: {total_alerts}")

        if total_alerts == 0:
            print("No fraud alerts found. Nothing to load.")
            return

        # --------------------------------------------------
        # 2. CONNECT TO HBASE
        # --------------------------------------------------

        print("\nConnecting to HBase...")

        connection = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=10000,
            autoconnect=False
        )

        connection.open()

        print("HBase connection successful.")

        table = connection.table(HBASE_TABLE)

        # --------------------------------------------------
        # 3. INSERT ALERTS INTO HBASE
        # --------------------------------------------------

        print("\nWriting fraud alerts into HBase...")

        loaded_count = 0
        sample_row_key = None

        with table.batch(batch_size=100) as batch:

            for row in df.toLocalIterator():

                record = row.asDict(recursive=True)

                transaction_id = record.get("transaction_id")

                if transaction_id is None:
                    continue

                row_key = str(transaction_id).encode("utf-8")

                if sample_row_key is None:
                    sample_row_key = str(transaction_id)

                hbase_data = {}

                # Transaction column family
                for field in TRANSACTION_FIELDS:

                    if field in record:
                        value = convert_value(record[field])

                        if value is not None:
                            hbase_data[
                                b"transaction:" + field.encode("utf-8")
                            ] = value

                # Fraud column family
                for field in FRAUD_FIELDS:

                    if field in record:
                        value = convert_value(record[field])

                        if value is not None:
                            hbase_data[
                                b"fraud:" + field.encode("utf-8")
                            ] = value

                # Additional alert metadata
                hbase_data[b"fraud:alert_status"] = b"DETECTED"

                hbase_data[b"fraud:loaded_at"] = (
                    datetime.now(timezone.utc)
                    .isoformat()
                    .encode("utf-8")
                )

                batch.put(row_key, hbase_data)

                loaded_count += 1

        print(f"\nSuccessfully submitted {loaded_count} alerts.")

        # --------------------------------------------------
        # 4. VERIFY A SAMPLE RECORD
        # --------------------------------------------------

        if sample_row_key:

            print("\nVerifying a sample HBase record:")

            sample = table.row(sample_row_key.encode("utf-8"))

            print(f"Row key: {sample_row_key}")

            for column, value in sample.items():

                print(
                    f"{column.decode('utf-8')}: "
                    f"{value.decode('utf-8')}"
                )

        print("\n" + "=" * 60)
        print("HDFS TO HBASE LOAD COMPLETED")
        print("=" * 60)

        print(f"Source unique alerts: {total_alerts}")
        print(f"Alerts submitted: {loaded_count}")
        print(f"HBase table: {HBASE_TABLE}")

    finally:

        if connection is not None:
            connection.close()

        spark.stop()


if __name__ == "__main__":
    main()
