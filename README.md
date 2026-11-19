# Real-Time Banking Fraud Detection Using Big Data

An end-to-end Big Data project for banking transaction processing, fraud detection, machine learning, distributed storage, workflow orchestration, and interactive analytics.

## 1. Project Overview

This project demonstrates a banking fraud detection pipeline that processes synthetic banking transactions using Big Data technologies.

It combines Kafka for transaction ingestion, Apache Spark for processing and fraud detection, Hadoop HDFS for distributed storage, Hive for SQL analytics, HBase for fraud-alert storage, Machine Learning for transaction classification, Apache Airflow for workflow orchestration, and Power BI for dashboard visualization.

The project is designed as an educational demonstration using synthetic transaction data. Its results should not be interpreted as validated performance on real banking transactions.

## 2. Project Objectives

- Generate synthetic banking transaction data.
- Ingest transactions through Apache Kafka.
- Process and analyze transactions using Apache Spark.
- Apply rule-based fraud detection.
- Store processed data in Hadoop HDFS.
- Create Hive tables and perform SQL analytics.
- Store fraud alerts in HBase.
- Train and evaluate machine learning models.
- Orchestrate processing tasks using Apache Airflow.
- Visualize fraud monitoring metrics using Power BI.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data generation, integration and ML |
| Apache Kafka | Transaction ingestion |
| Apache Spark 3.5.9 | Batch and streaming processing |
| Hadoop HDFS 3.3.6 | Distributed data storage |
| Apache Hive 3.1.2 | SQL analytics and warehouse |
| Apache HBase 2.5.9 | Fraud-alert storage |
| Scikit-learn | Machine learning |
| Apache Airflow | Workflow orchestration |
| Power BI | Interactive dashboard |
| Docker | Kafka containerization |
| Git and GitHub | Version control and project hosting |

## 4. System Architecture

```text
       Synthetic Transaction Generator
                    |
                    v
             Apache Kafka
          banking-transactions
                    |
                    v
          Apache Spark Processing
           /                 \
          v                   v
   Fraud Detection       Data Processing
          |                   |
          v                   v
     Fraud Alerts          Hadoop HDFS
          |                   |
          v                   v
        HBase                 Hive
          |                   |
          |                   v
          |             SQL Analytics
          |                   |
          +-------------------+
                    |
                    v
              Power BI
          Fraud Monitoring Dashboard


       Machine Learning Pipeline
                    |
                    v
       Fraud Classification Model
                    |
                    v
         Prediction and Evaluation


          Apache Airflow
       Workflow Orchestration
```

Airflow coordinates the scheduled processing workflow. The machine learning training workflow is also included in the orchestration pipeline.

## 5. Dataset

The project uses a synthetic banking transaction dataset generated with Python.

### Original dataset

- Total transactions: 10,000
- Normal transactions: 9,592
- Fraud transactions: 408
- Fraud percentage: 4.08%

### Transaction fields

- transaction_id
- customer_id
- account_id
- transaction_amount
- transaction_type
- merchant_category
- transaction_location
- transaction_timestamp
- device_id
- transaction_status
- is_fraud

The original CSV is generated locally and is not included in the repository because generated raw datasets are excluded through `.gitignore`.

## 6. Fraud Detection Rules

The rule-based fraud detection component includes the following checks:

1. **High-value transaction:** Transaction amount greater than or equal to ₹100,000.
2. **Transaction velocity:** More than five transactions by a customer within two minutes.
3. **Location anomaly:** Transactions from multiple locations within two minutes.
4. **Repeated payment failures:** Three or more failed payments within two minutes.
5. **Unusual spending:** Transaction amount greater than ₹50,000 and more than three times the customer's average spending.

The current implementation evaluates certain rules within streaming micro-batches. It is an educational implementation and does not claim complete production-grade stateful fraud detection.

## 7. Hadoop HDFS and Hive

Processed transactions and fraud alerts are stored in HDFS.

The project uses these HDFS locations:

```text
/banking-fraud-detection/
    raw/
        transactions/
    processed/
        all_transactions/
        normal_transactions/
        fraud_transactions/
    alerts/
        fraud_alerts/
```

Hive external tables are created over the processed Parquet datasets.

SQL analytics include:

- Overall transaction statistics
- Actual versus detected fraud
- Fraud reasons
- Fraud by merchant category
- High-value transactions
- Customer risk analysis
- Daily fraud detection trends
- Failed transaction analysis
- Fraud by transaction type
- Fraud alert reports

## 8. HBase Fraud Alert Storage

Fraud alerts are loaded from HDFS into the HBase table:

```text
banking_fraud:fraud_alerts
```

The transaction ID is used as the row key.

Example column families:

- `transaction`
- `fraud`

Stored information includes transaction details, fraud status, fraud reason, alert status, and loading timestamp.

### Verified result

- Unique fraud alerts read from HDFS: 25
- Alerts submitted to HBase: 25
- HBase sample record successfully retrieved.

## 9. Machine Learning

The machine learning component uses Scikit-learn to train and evaluate two classification algorithms:

- Logistic Regression
- Random Forest

The dataset is divided into:

- Training set: 8,000 transactions
- Testing set: 2,000 transactions

### Evaluation results

| Metric | Logistic Regression | Random Forest |
|---|---:|---:|
| Accuracy | 100% | 100% |
| Precision | 100% | 100% |
| Recall | 100% | 100% |
| F1-score | 100% | 100% |
| ROC-AUC | 1.00 | 1.00 |
| PR-AUC | 1.00 | 1.00 |

Selected model: **Logistic Regression**

The model and evaluation outputs are saved under:

```text
machine-learning/
    models/
        best_fraud_model.joblib
    evaluation/
        evaluation_metrics.json
    predictions/
        test_predictions.csv
```

**Important evaluation note:** The perfect scores were obtained on the generated synthetic dataset. The generator assigns fraud and normal transactions using strongly separated transaction-amount ranges. This makes the classification task unusually easy and can cause target leakage through correlated features. These metrics should not be treated as evidence of real-world model performance. More realistic data, independent feature design, and additional validation are required for production use.

## 10. Apache Airflow Orchestration

The Airflow DAG is named:

```text
banking_fraud_detection_pipeline
```

It orchestrates five tasks:

1. `validate_environment`
2. `run_hive_analytics`
3. `run_spark_batch`
4. `train_ml_model`
5. `verify_outputs`

A manual DAG run completed successfully, with all five task instances reporting success.

## 11. Power BI Dashboard

The Power BI dashboard provides a consolidated view of banking fraud monitoring metrics.

It includes:

- Total transactions
- Detected fraud alerts
- Actual fraud transactions
- Fraud detection recall
- Detected fraud amount
- Daily fraud detection trend
- Fraud alerts by merchant category
- Fraud alerts by transaction type
- Fraud alerts by location
- Fraud alerts by transaction status

### Dashboard preview

The Power BI report is available at:

```text
powerbi/dashboard/banking.pbix
```

The dashboard uses the exported dataset:

```text
powerbi/dashboard/data/banking_fraud_transactions.csv
```

Open the `.pbix` file using Power BI Desktop to explore the report.

## 12. Verified Project Results

The following results were observed during project execution:

| Component | Result |
|---|---:|
| Original synthetic dataset | 10,000 transactions |
| Actual fraud labels in original dataset | 408 |
| HDFS processed transactions | 736 unique transactions |
| Rule-detected fraud alerts in HDFS | 25 |
| Fraud alerts submitted to HBase | 25 |
| Actual fraud labels in the 736-row HDFS dataset | 36 |
| Rule-based detection recall on that HDFS dataset | 69.44% |
| Machine learning training rows | 8,000 |
| Machine learning testing rows | 2,000 |
| Airflow DAG tasks | 5 successful |
| Selected ML model | Logistic Regression |

**Important:** The 736-row HDFS dataset is a subset of the original 10,000-row dataset. The HDFS fraud detection results and the machine learning test results refer to different datasets and should not be compared as if they were evaluated on the same records.

## 13. Repository Structure

```text
banking-fraud-detection/
│
├── airflow/
│   └── dags/
│
├── docker/
│
├── hadoop/
│   └── hdfs_operations/
│
├── hbase/
│   └── fraud_alerts/
│
├── hive/
│   ├── ddl/
│   └── queries/
│
├── machine-learning/
│   ├── training/
│   ├── evaluation/
│   ├── models/
│   └── predictions/
│
├── powerbi/
│   └── dashboard/
│       ├── data/
│       └── banking.pbix
│
├── spark/
│   ├── batch/
│   ├── streaming/
│   ├── transformations/
│   └── schemas/
│
├── transaction-generator/
│
├── data/
│   ├── raw/
│   ├── processed/
│   ├── reference/
│   └── sample/
│
├── docs/
│   ├── architecture/
│   └── screenshots/
│
├── tests/
├── config/
├── requirements.txt
├── requirements-ml.txt
├── .gitignore
└── README.md
```

Generated datasets, checkpoints, logs, and virtual environments are excluded from version control.

## 14. Environment

The project was developed and tested using a Windows and WSL Ubuntu environment.

Reported versions include:

- Windows PowerShell 5.1
- Ubuntu 26.04 LTS under WSL
- Python 3.13 on Windows
- Python 3.14 on WSL
- Java 11 for Hadoop and HBase
- Hadoop 3.3.6
- Spark 3.5.9
- Hive 3.1.2
- HBase 2.5.9
- Kafka 4.3.1 using Docker

The components are configured across Windows, WSL, and Docker. Commands and paths may need adjustment for other environments.

## 15. Setup and Execution

### Step 1: Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/banking-fraud-detection.git
cd banking-fraud-detection
```

### Step 2: Install Python dependencies

On Windows PowerShell:

```powershell
python -m venv .venv-ml
.\.venv-ml\Scripts\Activate.ps1
pip install -r requirements-ml.txt
```

### Step 3: Generate the synthetic dataset

```powershell
python transaction-generator/transaction_generator.py
```

### Step 4: Start Kafka

From the project root:

```powershell
docker compose -f docker/docker-compose.yml up -d
```

### Step 5: Run the Kafka producer

```powershell
python transaction-generator/kafka_producer.py
```

### Step 6: Run Spark processing

Run the batch and streaming scripts using the configured Spark environment.

```text
spark/batch/
spark/streaming/
```

### Step 7: Run Hive analytics and HBase alert loading

Use the SQL scripts under `hive/ddl/` and `hive/queries/`, and the Python scripts under `hbase/fraud_alerts/`, after starting and configuring the corresponding Hadoop services.

### Step 8: Train the machine learning model

```powershell
python machine-learning/training/train_fraud_model.py
```

### Step 9: Run Airflow

Start Airflow using the configured WSL virtual environment and Airflow home, then trigger:

```text
banking_fraud_detection_pipeline
```

### Step 10: Open the Power BI dashboard

Open:

```text
powerbi/dashboard/banking.pbix
```

using Power BI Desktop.

**Note:** The execution instructions are a high-level guide. Each distributed service must be configured and running before its dependent processing script is executed.

## 16. Limitations and Future Improvements

- Use realistic, publicly available or properly anonymized transaction data for stronger evaluation.
- Improve synthetic data generation to model customer-account ownership consistently.
- Introduce more realistic fraud patterns and overlapping transaction features.
- Use stateful streaming with event-time watermarks for transaction velocity detection.
- Improve Kafka ingestion reliability and verify complete transaction delivery.
- Process and reconcile the full 10,000-row dataset through the distributed pipeline.
- Evaluate ML models with leakage-resistant features and time-based validation.
- Add automated tests and production-style monitoring.
- Improve fault tolerance and resource management.
- Extend the dashboard with additional investigation and drill-through capabilities.

## 17. Project Status

Completed components include:

- Synthetic transaction generation
- Kafka setup and producer implementation
- Spark batch processing
- Spark Structured Streaming
- Rule-based fraud detection
- HDFS data lake integration
- Hive warehouse and SQL analytics
- HBase fraud-alert storage
- Machine learning model training and prediction
- Airflow workflow orchestration
- Power BI fraud monitoring dashboard

The project is an educational end-to-end Big Data demonstration, with further reliability, data-quality, and model-validation improvements identified for future work.

---

**Project:** Real-Time Banking Fraud Detection Using Big Data  
**Purpose:** Academic and mentor demonstration  
**Data:** Synthetic banking transactions