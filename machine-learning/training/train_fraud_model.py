
from pathlib import Path
import json
import warnings

import joblib
import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


warnings.filterwarnings("ignore")


# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "banking_transactions.csv"
)

MODEL_DIR = PROJECT_ROOT / "machine-learning" / "models"

EVALUATION_DIR = (
    PROJECT_ROOT
    / "machine-learning"
    / "evaluation"
)

PREDICTION_DIR = (
    PROJECT_ROOT
    / "machine-learning"
    / "predictions"
)

MODEL_DIR.mkdir(parents=True, exist_ok=True)
EVALUATION_DIR.mkdir(parents=True, exist_ok=True)
PREDICTION_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# 2. LOAD AND VALIDATE DATA
# --------------------------------------------------

def load_data():

    print("\nLoading banking transaction dataset...")

    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATA_PATH}"
        )

    df = pd.read_csv(DATA_PATH)

    required_columns = [
        "transaction_id",
        "transaction_amount",
        "transaction_type",
        "merchant_category",
        "transaction_location",
        "transaction_timestamp",
        "is_fraud",
    ]

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing columns: {missing_columns}"
        )

    print("Original rows:", len(df))

    df = df.drop_duplicates(
        subset=["transaction_id"]
    ).copy()

    df["transaction_amount"] = pd.to_numeric(
        df["transaction_amount"],
        errors="coerce"
    )

    df["transaction_timestamp"] = pd.to_datetime(
        df["transaction_timestamp"],
        errors="coerce"
    )

    df["is_fraud"] = pd.to_numeric(
        df["is_fraud"],
        errors="coerce"
    )

    df = df.dropna(
        subset=[
            "transaction_id",
            "transaction_amount",
            "transaction_timestamp",
            "is_fraud",
        ]
    ).copy()

    df = df[
        df["is_fraud"].isin([0, 1])
    ].copy()

    df["is_fraud"] = df["is_fraud"].astype(int)

    if df["is_fraud"].nunique() != 2:
        raise ValueError(
            "Training requires both normal and fraud records."
        )

    if df["is_fraud"].value_counts().min() < 2:
        raise ValueError(
            "Not enough examples in one of the target classes."
        )

    print("Clean rows:", len(df))
    print("\nTarget distribution:")
    print(df["is_fraud"].value_counts())

    print("\nFraud percentage:")
    print(round(df["is_fraud"].mean() * 100, 2), "%")

    return df


# --------------------------------------------------
# 3. FEATURE ENGINEERING
# --------------------------------------------------

def prepare_features(df):

    print("\nPerforming feature engineering...")

    data = df.copy()

    data["transaction_hour"] = (
        data["transaction_timestamp"].dt.hour
    )

    data["transaction_day_of_week"] = (
        data["transaction_timestamp"].dt.dayofweek
    )

    data["is_weekend"] = (
        data["transaction_day_of_week"] >= 5
    ).astype(int)

    # Keep identifiers separately for prediction output.
    transaction_ids = data["transaction_id"].copy()

    # Do not include the target or identifier columns
    # in model training.
    X = data[
        [
            "transaction_amount",
            "transaction_type",
            "merchant_category",
            "transaction_location",
            "transaction_hour",
            "transaction_day_of_week",
            "is_weekend",
        ]
    ].copy()

    y = data["is_fraud"].copy()

    numeric_features = [
        "transaction_amount",
        "transaction_hour",
        "transaction_day_of_week",
        "is_weekend",
    ]

    categorical_features = [
        "transaction_type",
        "merchant_category",
        "transaction_location",
    ]

    return (
        X,
        y,
        transaction_ids,
        numeric_features,
        categorical_features,
    )


# --------------------------------------------------
# 4. PREPROCESSING PIPELINE
# --------------------------------------------------

def build_preprocessor(
    numeric_features,
    categorical_features
):

    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_pipeline = Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(strategy="most_frequent")
            ),
            (
                "encoder",
                OneHotEncoder(handle_unknown="ignore")
            ),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "numeric",
                numeric_pipeline,
                numeric_features
            ),
            (
                "categorical",
                categorical_pipeline,
                categorical_features
            ),
        ]
    )

    return preprocessor


# --------------------------------------------------
# 5. TRAIN AND EVALUATE MODELS
# --------------------------------------------------

def evaluate_model(name, model, X_test, y_test):

    predictions = model.predict(X_test)

    probabilities = model.predict_proba(X_test)[:, 1]

    metrics = {
        "model": name,
        "accuracy": float(
            accuracy_score(y_test, predictions)
        ),
        "precision": float(
            precision_score(
                y_test,
                predictions,
                zero_division=0
            )
        ),
        "recall": float(
            recall_score(
                y_test,
                predictions,
                zero_division=0
            )
        ),
        "f1_score": float(
            f1_score(
                y_test,
                predictions,
                zero_division=0
            )
        ),
        "roc_auc": float(
            roc_auc_score(y_test, probabilities)
        ),
        "pr_auc": float(
            average_precision_score(
                y_test,
                probabilities
            )
        ),
        "confusion_matrix": (
            confusion_matrix(
                y_test,
                predictions,
                labels=[0, 1]
            ).tolist()
        ),
        "classification_report": (
            classification_report(
                y_test,
                predictions,
                labels=[0, 1],
                target_names=["Normal", "Fraud"],
                zero_division=0,
                output_dict=True
            )
        ),
    }

    print("\n" + "=" * 55)
    print(name)
    print("=" * 55)

    print("Accuracy:", round(metrics["accuracy"], 4))
    print("Precision:", round(metrics["precision"], 4))
    print("Recall:", round(metrics["recall"], 4))
    print("F1:", round(metrics["f1_score"], 4))
    print("ROC-AUC:", round(metrics["roc_auc"], 4))
    print("PR-AUC:", round(metrics["pr_auc"], 4))

    print("\nConfusion matrix:")
    print(metrics["confusion_matrix"])

    print("\nClassification report:")
    print(
        classification_report(
            y_test,
            predictions,
            labels=[0, 1],
            target_names=["Normal", "Fraud"],
            zero_division=0
        )
    )

    return metrics, predictions, probabilities


# --------------------------------------------------
# 6. MAIN TRAINING WORKFLOW
# --------------------------------------------------

def main():

    df = load_data()

    (
        X,
        y,
        transaction_ids,
        numeric_features,
        categorical_features,
    ) = prepare_features(df)

    # Stratification preserves the fraud ratio
    # in training and test data.
    X_train, X_test, y_train, y_test = (
        train_test_split(
            X,
            y,
            test_size=0.20,
            random_state=42,
            stratify=y
        )
    )

    print("\nTraining rows:", len(X_train))
    print("Testing rows:", len(X_test))

    preprocessor = build_preprocessor(
        numeric_features,
        categorical_features
    )

    models = {
        "Logistic Regression": LogisticRegression(
            max_iter=2000,
            class_weight="balanced",
            random_state=42
        ),

        "Random Forest": RandomForestClassifier(
            n_estimators=200,
            max_depth=12,
            min_samples_split=5,
            min_samples_leaf=2,
            class_weight="balanced_subsample",
            random_state=42,
            n_jobs=-1
        ),
    }

    results = []
    trained_models = {}

    for name, classifier in models.items():

        print("\nTraining:", name)

        pipeline = Pipeline(
            steps=[
                ("preprocessor", build_preprocessor(
                    numeric_features,
                    categorical_features
                )),
                ("classifier", classifier),
            ]
        )

        pipeline.fit(X_train, y_train)

        metrics, predictions, probabilities = (
            evaluate_model(
                name,
                pipeline,
                X_test,
                y_test
            )
        )

        results.append(metrics)

        trained_models[name] = (
            pipeline,
            predictions,
            probabilities
        )

    # Select using fraud-class F1 score.
    # PR-AUC is also reported because the dataset
    # is imbalanced.
    best_result = max(
        results,
        key=lambda item: item["f1_score"]
    )

    best_name = best_result["model"]

    best_model, best_predictions, best_probabilities = (
        trained_models[best_name]
    )

    print("\nBest model:", best_name)

    # --------------------------------------------------
    # 7. SAVE MODEL
    # --------------------------------------------------

    model_path = (
        MODEL_DIR / "best_fraud_model.joblib"
    )

    joblib.dump(best_model, model_path)

    print("\nSaved model:", model_path)

    # --------------------------------------------------
    # 8. SAVE EVALUATION METRICS
    # --------------------------------------------------

    metrics_output = {
        "dataset": str(DATA_PATH),
        "total_rows": int(len(df)),
        "fraud_rows": int(y.sum()),
        "normal_rows": int((y == 0).sum()),
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "selected_model": best_name,
        "selection_metric": "fraud_class_f1_score",
        "models": results,
    }

    metrics_path = (
        EVALUATION_DIR / "evaluation_metrics.json"
    )

    with open(metrics_path, "w") as file:
        json.dump(
            metrics_output,
            file,
            indent=4
        )

    print("Saved metrics:", metrics_path)

    # --------------------------------------------------
    # 9. SAVE TEST PREDICTIONS
    # --------------------------------------------------

    test_ids = transaction_ids.loc[X_test.index]

    predictions_df = pd.DataFrame({
        "transaction_id": test_ids.values,
        "actual_is_fraud": y_test.values,
        "predicted_is_fraud": best_predictions,
        "fraud_probability": best_probabilities,
    })

    predictions_path = (
        PREDICTION_DIR / "test_predictions.csv"
    )

    predictions_df.to_csv(
        predictions_path,
        index=False
    )

    print("Saved predictions:", predictions_path)

    print("\n" + "=" * 55)
    print("MACHINE LEARNING TRAINING COMPLETED")
    print("=" * 55)

    print("Selected model:", best_name)
    print("Model file:", model_path)
    print("Metrics file:", metrics_path)
    print("Predictions file:", predictions_path)


if __name__ == "__main__":
    main()
