import happybase

connection = happybase.Connection(
    host="localhost",
    port=9090,
    timeout=10000,
    autoconnect=False
)

try:
    connection.open()

    print("Connected to HBase successfully!")

    tables = connection.tables()

    print("\nAvailable HBase tables:")

    for table in tables:
        print(table.decode("utf-8"))

    print("\nHBase connection test completed.")

finally:
    connection.close()