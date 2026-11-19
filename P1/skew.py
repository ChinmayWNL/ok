
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, rand, concat_ws, sum as _sum
)


spark = (
    SparkSession.builder
    .appName("SparkSkewPractical")
    .master("local[4]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Spark application started!")


number_of_records = 5_000_000
df = spark.range(number_of_records)

skewed_df = df.withColumn(
    "key",
    when(
        col("id") < number_of_records * 0.90,
        lit(0)
    ).otherwise(
        (col("id") % 999) + 1
    )
)

print("Skewed dataset created!")

print("\nKey distribution (Top 10):")
skewed_df.groupBy("key").count().orderBy(
    col("count").desc()
).show(10)

print("\nStarting SALTED GROUP BY (Data Skew Fix)...")

salted_df = skewed_df.withColumn(
    "salt", (rand() * 8).cast("int")
)

salted_df = salted_df.withColumn(
    "salted_key",
    concat_ws("_", col("key"), col("salt"))
)

partial_count = salted_df.groupBy(
    "salted_key", "key"
).count()

final_result = (
    partial_count
    .groupBy("key")
    .agg(_sum("count").alias("count"))
    .orderBy(col("count").desc())
)

final_result.collect()
final_result.show()

print("\nSALTED GROUP BY completed successfully!")

print("Open Spark UI: http://localhost:4040")

time.sleep(120)
spark.stop()