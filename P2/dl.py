
import os
import sys

os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip, DeltaTable

builder = (
    SparkSession.builder
    .appName("DeltaLakeDemo")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Spark started successfully!")

data = [
    (1, "Siddhant", 90),
    (2, "Shreya", 88),
    (3, "Shruti", 75),
    (4, "Chinmay", 90),
    (5, "Vishnu", 88),
    (6, "Nikita", 75)
]

df = spark.createDataFrame(data, ["id", "name", "marks"])

print("Student Data:")
df.show()

path = os.path.join(os.getcwd(), "delta-students")

df.write.format("delta").mode("overwrite").save(path)

print("Read back from Delta table:")
spark.read.format("delta").load(path).show()

dt = DeltaTable.forPath(spark, path)
dt.update(
    condition="name = 'Nikita'",
    set={"marks": "'80'"}
)

print("After update:")
spark.read.format("delta").load(path).show()

print("Version 0 (before update):")
spark.read.format("delta").option(
    "versionAsOf", 0
).load(path).show()

print("Table history:")
dt.history().select(
    "version", "timestamp", "operation"
).show(truncate=False)

spark.stop()
