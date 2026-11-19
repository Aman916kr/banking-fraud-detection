
from pathlib import Path
import sys

import joblib
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

MODEL_PATH = (
    PROJECT_ROOT
    / "machine-learning"
    / "models"
    / "best_fraud_model.joblib"
)


def predict_transaction(transaction):

    model = joblib.load(MODEL_PATH)

    df = pd.DataFrame([transaction])

    df["transaction_timestamp"] = pd.to_datetime(
        df["transaction_timestamp"],
        errors="raise"
    )

    df["transaction_hour"] = (
        df["transaction_timestamp"].dt.hour
    )

    df["transaction_day_of_week"] = (
        df["transaction_timestamp"].dt.dayofweek
    )

    df["is_weekend"] = (
        df["transaction_day_of_week"] >= 5
    ).astype(int)

    features = df[
        [
            "transaction_amount",
            "transaction_type",
            "merchant_category",
            "transaction_location",
            "transaction_hour",
            "transaction_day_of_week",
            "is_weekend",
        ]
    ]

    prediction = int(model.predict(features)[0])

    probability = float(
        model.predict_proba(features)[0][1]
    )

    return {
        "predicted_is_fraud": prediction,
        "fraud_probability": round(probability, 4),
        "risk_level": (
            "FRAUD" if prediction == 1 else "NORMAL"
        )
    }


if __name__ == "__main__":

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            "Train the model before running predictions."
        )

    # Example transaction.
    # These values are illustrative, not a real customer.
    transaction = {
        "transaction_amount": 235819.72,
        "transaction_type": "ATM",
        "merchant_category": "Entertainment",
        "transaction_location": "Mumbai",
        "transaction_timestamp": "2026-10-29 10:27:08",
    }

    result = predict_transaction(transaction)

    print("\nMACHINE LEARNING FRAUD PREDICTION")
    print("-" * 40)

    for key, value in result.items():
        print(f"{key}: {value}")
