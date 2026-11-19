
from pathlib import Path
import shutil
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


PROJECT_ROOT = Path(
    "/mnt/c/BigDataProjects/banking-fraud-detection"
)

HDFS_INPUT = (
    "hdfs://localhost:9000/"
    "banking-fraud-detection/processed/all_transactions"
)

OUTPUT_DIR = PROJECT_ROOT / "powerbi" / "dashboard" / "data"

OUTPUT_FILE = OUTPUT_DIR / "banking_fraud_transactions.csv"


def main():

    print("=" * 60)
    print("POWER BI DATA EXPORT")
    print("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("BankingFraudPowerBIExport")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("\nReading transactions from HDFS...")

        df = spark.read.parquet(HDFS_INPUT)

        required_columns = [
            "transaction_id",
            "customer_id",
            "account_id",
            "transaction_amount",
            "transaction_type",
            "merchant_category",
            "transaction_location",
            "transaction_timestamp",
            "transaction_status",
            "is_fraud",
            "rule_flagged_fraud",
            "fraud_reason",
            "transaction_date",
        ]

        missing = [
            name for name in required_columns
            if name not in df.columns
        ]

        if missing:
            raise ValueError(
                f"Missing required columns: {missing}"
            )

        df = df.select(*required_columns)

        df = (
            df
            .withColumn(
                "transaction_amount",
                col("transaction_amount").cast("double")
            )
            .withColumn(
                "is_fraud",
                col("is_fraud").cast("boolean")
            )
            .withColumn(
                "rule_flagged_fraud",
                col("rule_flagged_fraud").cast("boolean")
            )
            .withColumn(
                "transaction_timestamp",
                col("transaction_timestamp").cast("timestamp")
            )
            .withColumn(
                "transaction_date",
                to_date(col("transaction_date"))
            )
            .dropDuplicates(["transaction_id"])
        )

        total = df.count()

        actual_fraud = df.filter(
            col("is_fraud") == True
        ).count()

        detected_fraud = df.filter(
            col("rule_flagged_fraud") == True
        ).count()

        print("\nSOURCE DATA SUMMARY")
        print("-" * 40)
        print("Unique transactions:", total)
        print("Actual fraud labels:", actual_fraud)
        print("Rule-detected alerts:", detected_fraud)

        if total == 0:
            raise RuntimeError("No transactions found in HDFS.")

        print("\nExporting CSV...")

        temp_dir = OUTPUT_DIR / "_powerbi_export_temp"

        if temp_dir.exists():
            shutil.rmtree(temp_dir)

        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .option("dateFormat", "yyyy-MM-dd") \
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
            .csv("file://" + str(temp_dir))

        part_files = list(temp_dir.glob("part-*.csv"))

        if len(part_files) != 1:
            raise RuntimeError(
                f"Expected one CSV part file, found {len(part_files)}"
            )

        if OUTPUT_FILE.exists():
            OUTPUT_FILE.unlink()

        shutil.move(
            str(part_files[0]),
            str(OUTPUT_FILE)
        )

        shutil.rmtree(temp_dir)

        print("\nEXPORT COMPLETED")
        print("-" * 40)
        print("Output:", OUTPUT_FILE)
        print("Exported rows:", total)
        print("File size:", round(OUTPUT_FILE.stat().st_size / 1024, 2), "KB")

        print("\nPOWER BI DATA EXPORT SUCCESSFUL.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
