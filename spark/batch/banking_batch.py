
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    lit,
    lower,
    trim,
    when,
    sum as spark_sum,
    round as spark_round,
    to_timestamp,
    to_date
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# --------------------------------------------------
# 1. PROJECT PATHS
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = (
    PROJECT_ROOT / "data" / "raw" / "banking_transactions.csv"
)

OUTPUT_ROOT = (
    PROJECT_ROOT / "data" / "processed" / "batch"
)


# --------------------------------------------------
# 2. DEFINE CSV SCHEMA
# --------------------------------------------------

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_amount", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("transaction_location", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("is_fraud", StringType(), True)
])


# --------------------------------------------------
# 3. CREATE SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("BankingFraudBatchProcessing")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def main():

    print("\n====================================")
    print(" BANKING FRAUD BATCH PROCESSING")
    print("====================================\n")

    # --------------------------------------------------
    # 4. READ RAW CSV DATA
    # --------------------------------------------------

    print("STEP 1: Reading transaction dataset")

    raw_df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv("file://" + str(INPUT_FILE))
    )

    print("Raw transaction count:", raw_df.count())

    raw_df.show(5, truncate=False)

    # --------------------------------------------------
    # 5. DATA CLEANING
    # --------------------------------------------------

    print("\nSTEP 2: Cleaning transaction data")

    cleaned_df = (
        raw_df
        .withColumn(
            "transaction_amount",
            col("transaction_amount").cast("double")
        )
        .withColumn(
            "transaction_timestamp",
            to_timestamp(col("transaction_timestamp"))
        )
        .withColumn(
            "is_fraud",
            lower(trim(col("is_fraud"))).isin(
                "1", "true", "yes"
            )
        )
    )

    # Remove records with invalid mandatory fields
    cleaned_df = cleaned_df.filter(
        col("transaction_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("transaction_amount").isNotNull()
        & (col("transaction_amount") > 0)
        & col("transaction_timestamp").isNotNull()
    )

    # Remove duplicate transactions
    cleaned_df = cleaned_df.dropDuplicates(
        ["transaction_id"]
    )

    # Add transaction date for partitioning
    cleaned_df = cleaned_df.withColumn(
        "transaction_date",
        to_date(col("transaction_timestamp"))
    )

    cleaned_df.cache()

    total_transactions = cleaned_df.count()

    print("Cleaned transaction count:", total_transactions)

    print("\nCleaned data:")
    cleaned_df.show(10, truncate=False)

    # --------------------------------------------------
    # 6. FRAUD DETECTION ANALYTICS
    # --------------------------------------------------

    print("\nSTEP 3: Calculating fraud statistics")

    fraud_summary = cleaned_df.agg(
        count("*").alias("total_transactions"),

        spark_sum(
            when(col("is_fraud"), 1).otherwise(0)
        ).alias("fraud_transactions"),

        spark_sum(
            when(~col("is_fraud"), 1).otherwise(0)
        ).alias("normal_transactions"),

        spark_sum("transaction_amount").alias(
            "total_transaction_amount"
        ),

        spark_sum(
            when(
                col("is_fraud"),
                col("transaction_amount")
            ).otherwise(0)
        ).alias("fraud_transaction_amount")
    )

    fraud_summary = fraud_summary.withColumn(
        "fraud_percentage",
        spark_round(
            col("fraud_transactions")
            / col("total_transactions") * 100,
            2
        )
    )

    fraud_summary.show(truncate=False)

    # --------------------------------------------------
    # 7. MERCHANT CATEGORY ANALYSIS
    # --------------------------------------------------

    print("\nSTEP 4: Merchant category analysis")

    merchant_analysis = (
        cleaned_df
        .groupBy("merchant_category")
        .agg(
            count("*").alias("total_transactions"),

            spark_sum(
                when(col("is_fraud"), 1).otherwise(0)
            ).alias("fraud_transactions"),

            spark_sum("transaction_amount").alias(
                "total_amount"
            )
        )
        .orderBy(col("fraud_transactions").desc())
    )

    merchant_analysis.show(truncate=False)

    # --------------------------------------------------
    # 8. CUSTOMER RISK ANALYSIS
    # --------------------------------------------------

    print("\nSTEP 5: Customer fraud analysis")

    customer_risk = (
        cleaned_df
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_transactions"),

            spark_sum(
                when(col("is_fraud"), 1).otherwise(0)
            ).alias("fraud_transactions"),

            spark_sum("transaction_amount").alias(
                "total_amount"
            )
        )
        .filter(col("fraud_transactions") > 0)
        .orderBy(col("fraud_transactions").desc())
    )

    customer_risk.show(20, truncate=False)

    # --------------------------------------------------
    # 9. SAVE PROCESSED DATA AS PARQUET
    # --------------------------------------------------

    print("\nSTEP 6: Saving processed Parquet data")

    cleaned_df.write \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .parquet((OUTPUT_ROOT / "clean_transactions").as_uri())

    fraud_summary.write \
        .mode("overwrite") \
        .parquet((OUTPUT_ROOT / "fraud_summary").as_uri())

    merchant_analysis.write \
        .mode("overwrite") \
        .parquet((OUTPUT_ROOT / "merchant_analysis").as_uri())

    customer_risk.write \
        .mode("overwrite") \
        .parquet((OUTPUT_ROOT / "customer_risk").as_uri())

    print("\nAll Parquet outputs saved successfully.")

    # --------------------------------------------------
    # 10. VERIFY SAVED DATA
    # --------------------------------------------------

    print("\nSTEP 7: Verifying saved Parquet data")

    verified_df = spark.read.parquet(
        (OUTPUT_ROOT / "clean_transactions").as_uri()
    )

    print("Verified Parquet row count:", verified_df.count())

    verified_df.printSchema()

    print("\nBATCH PROCESSING COMPLETED SUCCESSFULLY!")

    cleaned_df.unpersist()


if __name__ == "__main__":

    try:
        main()

    finally:
        spark.stop()
