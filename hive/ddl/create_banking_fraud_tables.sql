
CREATE DATABASE IF NOT EXISTS banking_fraud;

USE banking_fraud;

-- 1. Master transaction table.
-- transaction_date is an HDFS partition column.

CREATE EXTERNAL TABLE IF NOT EXISTS all_transactions (
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    transaction_amount DOUBLE,
    transaction_type STRING,
    merchant_category STRING,
    transaction_location STRING,
    transaction_timestamp TIMESTAMP,
    device_id STRING,
    transaction_status STRING,
    is_fraud BOOLEAN,
    transactions_2m BIGINT,
    location_count_2m INT,
    failed_payments_2m BIGINT,
    high_value_flag BOOLEAN,
    velocity_flag BOOLEAN,
    location_anomaly_flag BOOLEAN,
    repeated_failure_flag BOOLEAN,
    unusual_spending_flag BOOLEAN,
    rule_flagged_fraud BOOLEAN,
    fraud_reason STRING
)
PARTITIONED BY (
    transaction_date DATE
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/banking-fraud-detection/processed/all_transactions';


-- 2. Normal transaction table.

CREATE EXTERNAL TABLE IF NOT EXISTS normal_transactions (
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    transaction_amount DOUBLE,
    transaction_type STRING,
    merchant_category STRING,
    transaction_location STRING,
    transaction_timestamp TIMESTAMP,
    device_id STRING,
    transaction_status STRING,
    is_fraud BOOLEAN,
    transaction_date DATE,
    transactions_2m BIGINT,
    location_count_2m INT,
    failed_payments_2m BIGINT,
    high_value_flag BOOLEAN,
    velocity_flag BOOLEAN,
    location_anomaly_flag BOOLEAN,
    repeated_failure_flag BOOLEAN,
    unusual_spending_flag BOOLEAN,
    rule_flagged_fraud BOOLEAN,
    fraud_reason STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/banking-fraud-detection/processed/normal_transactions';


-- 3. Fraud transaction table.

CREATE EXTERNAL TABLE IF NOT EXISTS fraud_transactions (
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    transaction_amount DOUBLE,
    transaction_type STRING,
    merchant_category STRING,
    transaction_location STRING,
    transaction_timestamp TIMESTAMP,
    device_id STRING,
    transaction_status STRING,
    is_fraud BOOLEAN,
    transaction_date DATE,
    transactions_2m BIGINT,
    location_count_2m INT,
    failed_payments_2m BIGINT,
    high_value_flag BOOLEAN,
    velocity_flag BOOLEAN,
    location_anomaly_flag BOOLEAN,
    repeated_failure_flag BOOLEAN,
    unusual_spending_flag BOOLEAN,
    rule_flagged_fraud BOOLEAN,
    fraud_reason STRING
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/banking-fraud-detection/processed/fraud_transactions';


-- 4. Fraud alert table.

CREATE EXTERNAL TABLE IF NOT EXISTS fraud_alerts (
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    transaction_amount DOUBLE,
    transaction_type STRING,
    merchant_category STRING,
    transaction_location STRING,
    transaction_timestamp TIMESTAMP,
    transaction_date DATE,
    transaction_status STRING,
    fraud_reason STRING,
    rule_flagged_fraud BOOLEAN,
    is_fraud BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/banking-fraud-detection/alerts/fraud_alerts';


-- Discover date partitions in the master table.

MSCK REPAIR TABLE all_transactions;

SHOW TABLES;
