
USE banking_fraud;

-- ==========================================
-- QUERY 1: Overall transaction statistics
-- ==========================================

SELECT
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT transaction_id) AS unique_transactions,
    SUM(CASE WHEN rule_flagged_fraud THEN 1 ELSE 0 END)
        AS detected_fraud_transactions,
    SUM(CASE WHEN NOT rule_flagged_fraud THEN 1 ELSE 0 END)
        AS normal_transactions,
    ROUND(
        100.0 * SUM(CASE WHEN rule_flagged_fraud THEN 1 ELSE 0 END)
        / COUNT(*),
        2
    ) AS detected_fraud_percentage
FROM all_transactions;


-- ==========================================
-- QUERY 2: Actual fraud vs detected fraud
-- ==========================================

SELECT
    is_fraud,
    rule_flagged_fraud,
    COUNT(*) AS transaction_count
FROM all_transactions
GROUP BY is_fraud, rule_flagged_fraud
ORDER BY is_fraud, rule_flagged_fraud;


-- ==========================================
-- QUERY 3: Fraud reasons distribution
-- ==========================================

SELECT
    fraud_reason,
    COUNT(*) AS flagged_transactions
FROM fraud_transactions
GROUP BY fraud_reason
ORDER BY flagged_transactions DESC;


-- ==========================================
-- QUERY 4: Fraud by merchant category
-- ==========================================

SELECT
    merchant_category,
    COUNT(*) AS total_transactions,
    SUM(
        CASE WHEN rule_flagged_fraud THEN 1 ELSE 0 END
    ) AS fraud_transactions,
    ROUND(
        100.0 * SUM(
            CASE WHEN rule_flagged_fraud THEN 1 ELSE 0 END
        ) / COUNT(*),
        2
    ) AS fraud_percentage
FROM all_transactions
GROUP BY merchant_category
ORDER BY fraud_transactions DESC;


-- ==========================================
-- QUERY 5: High-value transactions
-- ==========================================

SELECT
    transaction_id,
    customer_id,
    transaction_amount,
    merchant_category,
    transaction_location,
    transaction_timestamp,
    fraud_reason
FROM fraud_transactions
WHERE high_value_flag = TRUE
ORDER BY transaction_amount DESC;


-- ==========================================
-- QUERY 6: Customer risk analysis
-- ==========================================

SELECT
    customer_id,
    COUNT(*) AS total_transactions,
    SUM(
        CASE WHEN rule_flagged_fraud THEN 1 ELSE 0 END
    ) AS flagged_transactions,
    ROUND(SUM(transaction_amount), 2) AS total_transaction_amount,
    ROUND(
        SUM(
            CASE
                WHEN rule_flagged_fraud THEN transaction_amount
                ELSE 0
            END
        ),
        2
    ) AS flagged_transaction_amount
FROM all_transactions
GROUP BY customer_id
HAVING SUM(
    CASE WHEN rule_flagged_fraud THEN 1 ELSE 0 END
) > 0
ORDER BY flagged_transactions DESC, flagged_transaction_amount DESC;


-- ==========================================
-- QUERY 7: Daily transaction trend
-- ==========================================

SELECT
    transaction_date,
    COUNT(*) AS total_transactions,
    SUM(
        CASE WHEN rule_flagged_fraud THEN 1 ELSE 0 END
    ) AS fraud_transactions,
    SUM(
        CASE WHEN NOT rule_flagged_fraud THEN 1 ELSE 0 END
    ) AS normal_transactions
FROM all_transactions
GROUP BY transaction_date
ORDER BY transaction_date;


-- ==========================================
-- QUERY 8: Failed payment analysis
-- ==========================================

SELECT
    transaction_status,
    COUNT(*) AS transaction_count,
    SUM(
        CASE WHEN rule_flagged_fraud THEN 1 ELSE 0 END
    ) AS flagged_transactions
FROM all_transactions
GROUP BY transaction_status
ORDER BY transaction_count DESC;


-- ==========================================
-- QUERY 9: Fraud by transaction type
-- ==========================================

SELECT
    transaction_type,
    COUNT(*) AS total_transactions,
    SUM(
        CASE WHEN rule_flagged_fraud THEN 1 ELSE 0 END
    ) AS fraud_transactions
FROM all_transactions
GROUP BY transaction_type
ORDER BY fraud_transactions DESC;


-- ==========================================
-- QUERY 10: Fraud alert report
-- ==========================================

SELECT
    transaction_id,
    customer_id,
    transaction_amount,
    transaction_location,
    transaction_timestamp,
    fraud_reason
FROM fraud_alerts
ORDER BY transaction_timestamp DESC;
