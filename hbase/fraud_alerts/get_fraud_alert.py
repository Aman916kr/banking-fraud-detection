
import sys
import happybase

if len(sys.argv) != 2:
    print(
        "Usage: python get_fraud_alert.py "
        "<transaction_id>"
    )
    sys.exit(1)

transaction_id = sys.argv[1]

connection = happybase.Connection(
    host="localhost",
    port=9090,
    timeout=10000
)

try:

    table = connection.table(
        "banking_fraud:fraud_alerts"
    )

    result = table.row(
        transaction_id.encode("utf-8")
    )

    if not result:
        print("No fraud alert found.")
    else:
        print("\nFRAUD ALERT DETAILS")
        print("-" * 40)

        for column, value in result.items():

            print(
                f"{column.decode('utf-8')}: "
                f"{value.decode('utf-8')}"
            )

finally:
    connection.close()
