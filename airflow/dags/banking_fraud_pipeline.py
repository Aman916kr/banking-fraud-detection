
from datetime import datetime, timezone
from pathlib import Path
import os
import shutil
import subprocess

from airflow.sdk import dag, task


# --------------------------------------------------
# PROJECT PATHS
# --------------------------------------------------

PROJECT_ROOT = Path("/mnt/c/BigDataProjects/banking-fraud-detection")

RAW_DATA = PROJECT_ROOT / "data/raw/banking_transactions.csv"

SPARK_BATCH_SCRIPT = (
    PROJECT_ROOT / "spark/batch/banking_batch.py"
)

HIVE_ANALYTICS_SCRIPT = (
    PROJECT_ROOT / "hive/queries/fraud_analytics.sql"
)

ML_TRAINING_SCRIPT = (
    PROJECT_ROOT / "machine-learning/training/train_fraud_model.py"
)

ML_PYTHON = PROJECT_ROOT / ".venv-ml/bin/python"

MODEL_PATH = (
    PROJECT_ROOT / "machine-learning/models/best_fraud_model.joblib"
)

METRICS_PATH = (
    PROJECT_ROOT / "machine-learning/evaluation/evaluation_metrics.json"
)

PREDICTIONS_PATH = (
    PROJECT_ROOT / "machine-learning/predictions/test_predictions.csv"
)

BATCH_OUTPUTS = [
    PROJECT_ROOT / "data/processed/batch/clean_transactions",
    PROJECT_ROOT / "data/processed/batch/fraud_summary",
    PROJECT_ROOT / "data/processed/batch/merchant_analysis",
    PROJECT_ROOT / "data/processed/batch/customer_risk",
]

HDFS_ALERT_PATH = "/banking-fraud-detection/alerts/fraud_alerts"


# --------------------------------------------------
# COMMAND EXECUTION
# --------------------------------------------------

def run_command(command, label):
    print(f"\nStarting: {label}")
    print("Command:", " ".join(map(str, command)))

    environment = os.environ.copy()

    subprocess.run(
        list(map(str, command)),
        cwd=str(PROJECT_ROOT),
        env=environment,
        check=True
    )

    print(f"\nCompleted: {label}")


# --------------------------------------------------
# AIRFLOW DAG
# --------------------------------------------------

@dag(
    dag_id="banking_fraud_detection_pipeline",
    description="Banking fraud detection using Spark, Hive and ML",
    start_date=datetime(2026, 11, 18, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["banking", "fraud-detection", "spark", "hive", "ml"],
)

def banking_fraud_detection_pipeline():

    # ----------------------------------------------
    # TASK 1: VALIDATE ENVIRONMENT
    # ----------------------------------------------

    @task
    def validate_environment():

        print("\nValidating project environment...")

        required_files = [
            RAW_DATA,
            SPARK_BATCH_SCRIPT,
            HIVE_ANALYTICS_SCRIPT,
            ML_TRAINING_SCRIPT,
            ML_PYTHON,
        ]

        for file_path in required_files:
            if not file_path.exists():
                raise FileNotFoundError(
                    f"Required file not found: {file_path}"
                )

            print("Verified:", file_path)

        required_commands = [
            "hdfs",
            "spark-submit",
            "hive",
        ]

        for command in required_commands:
            if shutil.which(command) is None:
                raise RuntimeError(
                    f"Required command not found: {command}"
                )

        # Verify existing HDFS alert directory
        run_command(
            ["hdfs", "dfs", "-test", "-e", HDFS_ALERT_PATH],
            "HDFS alert directory validation"
        )

        # Verify existing Hive tables without rebuilding them
        run_command(
            [
                "hive",
                "-S",
                "-e",
                (
                    "USE banking_fraud; "
                    "SHOW TABLES;"
                )
            ],
            "Existing Hive database validation"
        )

        print("\nEnvironment validation successful.")

    # ----------------------------------------------
    # TASK 2: SPARK BATCH PROCESSING
    # ----------------------------------------------

    @task
    def run_spark_batch():

        run_command(
            [
                "spark-submit",
                "--master", "local[2]",
                "--driver-memory", "512m",
                "--conf", "spark.sql.shuffle.partitions=2",
                str(SPARK_BATCH_SCRIPT),
            ],
            "Spark batch processing"
        )

        print("\nSpark batch processing completed.")

    # ----------------------------------------------
    # TASK 3: HIVE ANALYTICS
    # ----------------------------------------------

    @task
    def run_hive_analytics():

        run_command(
            [
                "hive",
                "--hiveconf", "hive.execution.engine=mr",
                "--hiveconf", "mapreduce.map.memory.mb=512",
                "--hiveconf", "mapreduce.reduce.memory.mb=512",
                "--hiveconf", "mapreduce.map.java.opts=-Xmx384m",
                "--hiveconf", "mapreduce.reduce.java.opts=-Xmx384m",
                "-f", str(HIVE_ANALYTICS_SCRIPT),
            ],
            "Hive fraud analytics"
        )

        print("\nHive analytics completed successfully.")

    # ----------------------------------------------
    # TASK 4: MACHINE LEARNING TRAINING
    # ----------------------------------------------

    @task
    def train_ml_model():

        backup_model = (
            PROJECT_ROOT
            / "machine-learning/models/baseline_windows_fraud_model.joblib"
        )

        if MODEL_PATH.exists() and not backup_model.exists():

            shutil.copy2(MODEL_PATH, backup_model)

            print("Original model backed up:", backup_model)

        run_command(
            [
                str(ML_PYTHON),
                str(ML_TRAINING_SCRIPT),
            ],
            "Machine Learning model training"
        )

        print("\nML model training completed.")

    # ----------------------------------------------
    # TASK 5: VERIFY ALL OUTPUTS
    # ----------------------------------------------

    @task
    def verify_outputs():

        print("\nVerifying pipeline outputs...")

        # Verify Spark Parquet outputs
        for output in BATCH_OUTPUTS:

            if not output.exists():
                raise FileNotFoundError(
                    f"Spark output missing: {output}"
                )

            print("Verified Spark output:", output)

        # Verify ML outputs
        ml_outputs = [
            MODEL_PATH,
            METRICS_PATH,
            PREDICTIONS_PATH,
        ]

        for output in ml_outputs:

            if not output.exists():
                raise FileNotFoundError(
                    f"ML output missing: {output}"
                )

            print("Verified ML output:", output)

        # Verify the existing Hive tables and query execution
        run_command(
            [
                "hive",
                "-S",
                "-e",
                (
                    "USE banking_fraud; "
                    "SELECT COUNT(*) FROM all_transactions; "
                    "SELECT COUNT(*) FROM fraud_transactions; "
                    "SELECT COUNT(*) FROM fraud_alerts;"
                ),
            ],
            "Hive table and data verification"
        )

        print("\nAll pipeline output checks passed.")

    # ----------------------------------------------
    # TASK DEPENDENCIES
    # ----------------------------------------------

    validation = validate_environment()

    spark_task = run_spark_batch()
    hive_task = run_hive_analytics()
    ml_task = train_ml_model()

    verification = verify_outputs()

    validation >> [spark_task, hive_task, ml_task]

    [spark_task, hive_task, ml_task] >> verification


banking_fraud_detection_pipeline()
