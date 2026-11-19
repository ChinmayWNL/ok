from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
import os
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
spark = SparkSession.builder.master("local[2]").appName("LiveWordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
words = lines.select(explode(split(lines.value, " ")).alias("word"))
word_counts = words.groupBy("word").count()
query = word_counts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()