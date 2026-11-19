
import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Project configuration
PROJECT_ROOT = Path(__file__).resolve().parent.parent

CSV_FILE = PROJECT_ROOT / "data" / "raw" / "banking_transactions.csv"

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9093"]
KAFKA_TOPIC = "banking-transactions"

# Producer settings
MAX_MESSAGES = 1000
DELAY_SECONDS = 0.01


def create_producer():
    """Create and return a Kafka producer."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1
    )


def convert_transaction(row):
    """Convert CSV values into appropriate Python data types."""

    row["transaction_amount"] = float(row["transaction_amount"])

    fraud_value = str(row["is_fraud"]).strip().lower()
    row["is_fraud"] = fraud_value in ("1", "true", "yes")

    return row


def publish_transactions():
    """Read transactions from CSV and publish them to Kafka."""

    if not CSV_FILE.exists():
        raise FileNotFoundError(
            f"Transaction dataset not found: {CSV_FILE}"
        )

    producer = create_producer()

    sent_count = 0
    failed_count = 0

    print("Connected to Kafka.")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Dataset: {CSV_FILE}")
    print("-" * 50)

    try:
        with open(CSV_FILE, "r", newline="", encoding="utf-8") as file:

            reader = csv.DictReader(file)

            for row in reader:

                if sent_count >= MAX_MESSAGES:
                    break

                transaction = convert_transaction(row)

                try:
                    producer.send(
                        KAFKA_TOPIC,
                        key=transaction["transaction_id"],
                        value=transaction
                    ).get(timeout=10)

                    sent_count += 1

                    if sent_count % 100 == 0:
                        print(
                            f"Published {sent_count} transactions"
                        )

                    time.sleep(DELAY_SECONDS)

                except KafkaError as error:
                    failed_count += 1
                    print(f"Kafka publishing error: {error}")

    finally:
        producer.flush()
        producer.close()

    print("-" * 50)
    print("PRODUCER EXECUTION COMPLETED")
    print(f"Successfully published: {sent_count}")
    print(f"Failed messages: {failed_count}")


if __name__ == "__main__":
    publish_transactions()
