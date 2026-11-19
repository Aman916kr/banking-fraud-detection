from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ParquetWriteTest") \
    .master("local[2]") \
    .getOrCreate()

df = spark.range(1, 4)

output = "data/processed/streaming/parquet_test"

df.write.mode("overwrite").parquet(output)

print("Parquet write completed")
print("Rows verified:", spark.read.parquet(output).count())

spark.stop()
