
import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

# Reproducibility
random.seed(42)
np.random.seed(42)

# Configuration
TOTAL_TRANSACTIONS = 10000
FRAUD_RATE = 0.04

CUSTOMERS = [f"CUST{i:04d}" for i in range(1, 1001)]
ACCOUNTS = [f"ACC{i:04d}" for i in range(1, 1001)]

TRANSACTION_TYPES = ["UPI", "ATM", "CARD", "NEFT", "IMPS"]

MERCHANT_CATEGORIES = [
    "Shopping", "Food", "Travel",
    "Healthcare", "Entertainment",
    "Fuel", "Electronics"
]

CITIES = [
    "Hyderabad", "Mumbai", "Delhi",
    "Bangalore", "Chennai", "Pune",
    "Kolkata", "Gaya", "Patna"
]

STATUSES = ["SUCCESS", "FAILED"]

DEVICES = [f"DEV{i:04d}" for i in range(1, 501)]


def generate_transaction(index):

    is_fraud = 1 if random.random() < FRAUD_RATE else 0

    if is_fraud:
        amount = round(random.uniform(50000, 250000), 2)
        status = random.choices(
            STATUSES, weights=[0.85, 0.15]
        )[0]
    else:
        amount = round(random.uniform(100, 20000), 2)
        status = random.choices(
            STATUSES, weights=[0.95, 0.05]
        )[0]

    timestamp = datetime.now() - timedelta(
        minutes=random.randint(0, 60 * 24 * 30)
    )

    return {
        "transaction_id": f"TXN{index:07d}",
        "customer_id": random.choice(CUSTOMERS),
        "account_id": random.choice(ACCOUNTS),
        "transaction_amount": amount,
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "transaction_location": random.choice(CITIES),
        "transaction_timestamp": timestamp,
        "device_id": random.choice(DEVICES),
        "transaction_status": status,
        "is_fraud": is_fraud
    }


def generate_dataset():

    print("Generating banking transactions...")

    transactions = [
        generate_transaction(i)
        for i in range(1, TOTAL_TRANSACTIONS + 1)
    ]

    df = pd.DataFrame(transactions)

    # Create output directory
    output_dir = os.path.join("data", "raw")
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(
        output_dir, "banking_transactions.csv"
    )

    df.to_csv(output_file, index=False)

    print("\nDataset generated successfully!")
    print("Total transactions:", len(df))
    print("Fraud transactions:", int(df["is_fraud"].sum()))
    print("Normal transactions:", int((df["is_fraud"] == 0).sum()))
    print("Fraud percentage:", round(df["is_fraud"].mean() * 100, 2), "%")
    print("Output file:", output_file)

    print("\nFirst five transactions:")
    print(df.head())

    print("\nTransaction statistics:")
    print(df["transaction_amount"].describe())


if __name__ == "__main__":
    generate_dataset()
